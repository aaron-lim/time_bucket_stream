# frozen_string_literal: true

require "fileutils"
require "json"
require "securerandom"
require "set"
require "tmpdir"

require_relative "../../lib/time_bucket_stream"

module FileStreamStress
  WRITE_TIME = Time.utc(2026, 5, 6, 10, 15, 0)
  PROCESS_TIME = Time.utc(2026, 5, 6, 10, 17, 0)
  CRASH_EXIT_STATUS = 66

  Config = Struct.new(
    :path,
    :writers,
    :events_per_writer,
    :crashers,
    :processors,
    :max_rounds,
    :sync,
    :codec_name,
    :payload_bytes,
    :claim_grace,
    :keep_path,
    keyword_init: true
  ) do
    def expected_events
      writers * events_per_writer
    end
  end

  module_function

  def run(config)
    codec_for(config.codec_name)

    total_started_at = monotonic_time
    result_root = File.join(config.path, "stress_results")
    FileUtils.mkdir_p(result_root)

    expected_tokens = expected_tokens_for(config)

    _write_result, write_seconds = measure { write_events(config, result_root) }
    crash_reports, crash_seconds = measure { crash_processors(config, result_root) }
    drain_reports, drain_seconds = measure { drain_events(config, result_root) }

    report = build_report(
      config,
      expected_tokens,
      crash_reports,
      drain_reports,
      {
        "write_seconds" => write_seconds,
        "crash_seconds" => crash_seconds,
        "drain_seconds" => drain_seconds,
        "total_seconds" => monotonic_time - total_started_at
      }
    )
    assert_success!(report)

    report
  end

  def config_from_env
    Config.new(
      path: ENV.fetch("STREAM_PATH", Dir.mktmpdir("time-bucket-stream-stress")),
      writers: env_integer("WRITERS", 12, minimum: 1),
      events_per_writer: env_integer("EVENTS_PER_WRITER", 1_000, minimum: 1),
      crashers: env_integer("CRASHERS", 3, minimum: 0),
      processors: env_integer("PROCESSORS", 6, minimum: 1),
      max_rounds: env_integer("MAX_ROUNDS", 10, minimum: 1),
      sync: ENV.fetch("SYNC", "flush").to_sym,
      codec_name: ENV.fetch("CODEC", "json"),
      payload_bytes: env_integer("PAYLOAD_BYTES", 0, minimum: 0),
      claim_grace: env_integer("CLAIM_GRACE", TimeBucketStream::DEFAULT_CLAIM_GRACE, minimum: 0),
      keep_path: ENV["KEEP_PATH"] == "1"
    )
  end

  def env_integer(name, default, minimum:)
    value = Integer(ENV.fetch(name, default.to_s))
    return value if value >= minimum

    raise ArgumentError, "#{name} must be >= #{minimum}"
  rescue ArgumentError, TypeError
    raise ArgumentError, "#{name} must be an integer >= #{minimum}"
  end

  def expected_tokens_for(config)
    Set.new.tap do |tokens|
      config.writers.times do |writer_index|
        config.events_per_writer.times do |event_index|
          tokens << event_token(writer_index, event_index)
        end
      end
    end
  end

  def write_events(config, result_root)
    pids = fork_group(config.writers, result_root, "writer") do |writer_index|
      stream = build_stream(config, WRITE_TIME)

      config.events_per_writer.times do |event_index|
        stream.append(
          event_payload(config, writer_index, event_index)
        )
      end

      stream.close
      write_json(result_path(result_root, "writer", writer_index), {
        "pid" => Process.pid,
        "events" => config.events_per_writer
      })
    ensure
      stream&.close
    end

    wait_for_success!(pids, "writer")
  end

  def crash_processors(config, result_root)
    return [] if config.crashers.zero?

    pids = fork_group(config.crashers, result_root, "crasher") do |crasher_index|
      stream = build_stream(config, PROCESS_TIME)
      batch = stream.read

      write_json(result_path(result_root, "crasher", crasher_index), {
        "pid" => Process.pid,
        "entries" => batch.length,
        "files" => entry_file_count(batch.entries)
      })

      exit! CRASH_EXIT_STATUS
    ensure
      stream&.close
    end

    statuses = wait_for_exit_status!(pids, "crasher", CRASH_EXIT_STATUS)
    reports = read_reports(result_root, "crasher", config.crashers)

    if reports.sum { |report| report.fetch("entries") }.zero?
      raise "crash processors did not claim any entries; stress test did not exercise crash recovery"
    end

    reports.each_with_index do |report, index|
      report["exitstatus"] = statuses.fetch(index).exitstatus
    end
  end

  def drain_events(config, result_root)
    reports = []

    config.max_rounds.times do |round|
      round_reports = run_drain_round(config, result_root, round)
      reports.concat(round_reports)

      return reports if round_reports.sum { |report| report.fetch("entries") }.zero?
    end

    raise "stream did not drain after #{config.max_rounds} rounds"
  end

  def run_drain_round(config, result_root, round)
    pids = fork_group(config.processors, result_root, "processor-#{round}") do |processor_index|
      stream = build_stream(config, PROCESS_TIME)
      batch = stream.read
      ids = batch.entries.map(&:first)
      tokens = batch.map { |payload| payload&.fetch("token") }

      write_json(result_path(result_root, "processor-#{round}", processor_index), {
        "pid" => Process.pid,
        "round" => round,
        "entries" => batch.length,
        "files" => entry_file_count(batch.entries),
        "ids" => ids,
        "tokens" => tokens
      })

      batch.delete
    ensure
      stream&.close
    end

    wait_for_success!(pids, "processor round #{round}")
    read_reports(result_root, "processor-#{round}", config.processors)
  end

  def build_report(config, expected_tokens, crash_reports, drain_reports, timings)
    tokens = drain_reports.flat_map { |report| report.fetch("tokens") }.compact
    actual_tokens = Set.new(tokens)
    duplicate_tokens = tokens.tally.select { |_token, count| count > 1 }
    paths = TimeBucketStream::Paths.new(path: config.path)

    {
      "path" => config.path,
      "codec" => config.codec_name,
      "payload_bytes" => config.payload_bytes,
      "writers" => config.writers,
      "events_per_writer" => config.events_per_writer,
      "expected_events" => expected_tokens.length,
      "processed_events" => tokens.length,
      "unique_processed_events" => actual_tokens.length,
      "missing_events" => (expected_tokens - actual_tokens).to_a.sort,
      "unexpected_events" => (actual_tokens - expected_tokens).to_a.sort,
      "duplicate_events" => duplicate_tokens,
      "crashers" => crash_reports,
      "drain_rounds" => summarize_rounds(drain_reports),
      "performance" => performance_summary(
        expected_events: expected_tokens.length,
        processed_events: tokens.length,
        timings: timings
      ),
      "leftover_logs" => Dir[File.join(paths.logs, "*.jsonl")].map { |path| File.basename(path) }.sort,
      "leftover_processing" => Dir[File.join(paths.processing, "*.jsonl")].map { |path| File.basename(path) }.sort,
      "leftover_quarantine" => Dir[File.join(paths.quarantine, "*.jsonl")].map { |path| File.basename(path) }.sort
    }
  end

  def assert_success!(report)
    failures = []
    failures << "missing events: #{report.fetch("missing_events").length}" unless report.fetch("missing_events").empty?
    failures << "unexpected events: #{report.fetch("unexpected_events").length}" unless report.fetch("unexpected_events").empty?
    failures << "duplicate events: #{report.fetch("duplicate_events").length}" unless report.fetch("duplicate_events").empty?
    failures << "leftover logs: #{report.fetch("leftover_logs").length}" unless report.fetch("leftover_logs").empty?
    failures << "leftover processing files: #{report.fetch("leftover_processing").length}" unless report.fetch("leftover_processing").empty?
    failures << "leftover quarantine files: #{report.fetch("leftover_quarantine").length}" unless report.fetch("leftover_quarantine").empty?

    return if failures.empty?

    raise "#{failures.join(", ")}\n#{JSON.pretty_generate(report)}"
  end

  def summarize_rounds(reports)
    reports
      .group_by { |report| report.fetch("round") }
      .map do |round, round_reports|
        {
          "round" => round,
          "processors" => round_reports.length,
          "entries" => round_reports.sum { |report| report.fetch("entries") },
          "files" => round_reports.sum { |report| report.fetch("files") }
        }
      end
      .sort_by { |summary| summary.fetch("round") }
  end

  def build_stream(config, time)
    TimeBucketStream.new(
      path: config.path,
      sync: config.sync,
      clock: -> { time },
      claim_grace: config.claim_grace,
      codec: codec_for(config.codec_name)
    )
  end

  def codec_for(name)
    case name.to_s
    when "json"
      TimeBucketStream::Codecs::Json.new
    when "oj"
      TimeBucketStream::Codecs::Oj.new
    else
      raise ArgumentError, "CODEC must be json or oj"
    end
  end

  def performance_summary(expected_events:, processed_events:, timings:)
    {
      "write_seconds" => rounded_seconds(timings.fetch("write_seconds")),
      "crash_seconds" => rounded_seconds(timings.fetch("crash_seconds")),
      "drain_seconds" => rounded_seconds(timings.fetch("drain_seconds")),
      "total_seconds" => rounded_seconds(timings.fetch("total_seconds")),
      "write_events_per_second" => rate(expected_events, timings.fetch("write_seconds")),
      "drain_events_per_second" => rate(processed_events, timings.fetch("drain_seconds")),
      "total_events_per_second" => rate(processed_events, timings.fetch("total_seconds"))
    }
  end

  def measure
    started_at = monotonic_time
    result = yield

    [result, monotonic_time - started_at]
  end

  def monotonic_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def rounded_seconds(seconds)
    seconds.round(4)
  end

  def rate(count, seconds)
    return nil unless seconds.positive?

    (count / seconds).round(1)
  end

  def fork_group(count, result_root, label)
    read_gate, write_gate = IO.pipe

    count.times.map do |index|
      Process.fork do
        write_gate.close
        read_gate.read(1)
        read_gate.close

        yield index
        exit! 0
      rescue SystemExit
        raise
      rescue => error
        write_json(error_path(result_root, label, index), {
          "pid" => Process.pid,
          "class" => error.class.name,
          "message" => error.message,
          "backtrace" => error.backtrace
        })
        exit! 1
      end
    end
  ensure
    read_gate&.close unless read_gate&.closed?

    if write_gate && !write_gate.closed?
      count.times { write_gate.write(".") }
      write_gate.close
    end
  end

  def wait_for_success!(pids, label)
    statuses = wait_for_processes(pids)
    failures = statuses.reject(&:success?)
    return statuses if failures.empty?

    raise "#{label} process failed: #{failures.map { |status| status.exitstatus || status.termsig }.join(", ")}"
  end

  def wait_for_exit_status!(pids, label, expected_status)
    statuses = wait_for_processes(pids)
    failures = statuses.reject { |status| status.exitstatus == expected_status }
    return statuses if failures.empty?

    raise "#{label} process had unexpected status: #{failures.map { |status| status.exitstatus || status.termsig }.join(", ")}"
  end

  def wait_for_processes(pids)
    pids.map do |pid|
      _waited_pid, status = Process.wait2(pid)
      status
    end
  end

  def read_reports(result_root, label, count)
    count.times.map do |index|
      path = result_path(result_root, label, index)
      File.exist?(path) ? JSON.parse(File.read(path)) : empty_report(label, index)
    end
  end

  def empty_report(label, index)
    {
      "label" => label,
      "index" => index,
      "entries" => 0,
      "files" => 0,
      "ids" => [],
      "tokens" => []
    }
  end

  def event_token(writer_index, event_index)
    "writer-#{writer_index}-event-#{event_index}"
  end

  def event_payload(config, writer_index, event_index)
    payload = {
      "token" => event_token(writer_index, event_index),
      "writer_index" => writer_index,
      "event_index" => event_index,
      "pid" => Process.pid
    }

    payload["data"] = "x" * config.payload_bytes if config.payload_bytes.positive?

    payload
  end

  def entry_file_count(entries)
    entries.map { |id, _payload| id.split(":", 2).first }.uniq.length
  end

  def write_json(path, payload)
    tmp_path = "#{path}.#{Process.pid}.tmp"

    FileUtils.mkdir_p(File.dirname(path))
    File.write(tmp_path, JSON.generate(payload))
    File.rename(tmp_path, path)
  end

  def result_path(result_root, label, index)
    File.join(result_root, "#{label}-#{index}.json")
  end

  def error_path(result_root, label, index)
    File.join(result_root, "#{label}-#{index}.error.json")
  end
end

if $PROGRAM_NAME == __FILE__
  config = FileStreamStress.config_from_env

  begin
    report = FileStreamStress.run(config)
    puts JSON.pretty_generate(report)
  ensure
    FileUtils.rm_rf(config.path) unless config.keep_path
  end
end
