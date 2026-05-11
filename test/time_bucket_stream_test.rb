# frozen_string_literal: true

require "fileutils"
require "json"
require "tmpdir"

require "test_helper"

class TimeBucketStreamTest < Minitest::Test
  def test_has_a_version_number
    refute_nil TimeBucketStream::VERSION
  end

  def test_full_constructor_returns_a_stream
    Dir.mktmpdir do |root|
      stream = TimeBucketStream.new(path: stream_path(root))

      assert_instance_of TimeBucketStream, stream
      refute_respond_to stream, :delete
      refute_respond_to stream, :release
    ensure
      stream&.close
    end
  end

  def test_constructor_rejects_blank_paths
    assert_raises(ArgumentError) do
      TimeBucketStream.new(path: " ")
    end
  end

  def test_writer_appends_json_lines_to_a_time_bucketed_process_file
    with_writer do |writer, root|
      id = writer.append("status" => "success")

      logs = log_files(root)
      assert_equal 1, logs.length
      assert_match(/\A\d{12}-.+-#{Process.pid}-[0-9a-f]{8}\.jsonl\z/, File.basename(logs.first))
      assert_equal 1, id

      entry = read_entries(logs.first).fetch(0)
      assert_equal id, entry.fetch("id")
      assert_equal({"status" => "success"}, entry.fetch("payload"))
    end
  end

  def test_writer_rotates_files_by_minute
    current_time = Time.utc(2026, 5, 6, 10, 15, 30)

    with_writer(clock: -> { current_time }) do |writer, root|
      writer.append("row" => 1)
      current_time = Time.utc(2026, 5, 6, 10, 16, 0)
      writer.append("row" => 2)

      log_names = log_files(root).map { |path| File.basename(path) }.sort

      assert_equal 2, log_names.length
      assert_match(/\A202605061015-/, log_names.fetch(0))
      assert_match(/\A202605061016-/, log_names.fetch(1))
    end
  end

  def test_writer_reopens_when_the_active_file_disappears
    with_writer(sync: :none) do |writer, root|
      writer.append("row" => 1)
      FileUtils.rm_f(log_files(root).fetch(0))

      writer.append("row" => 2)

      entries = log_files(root).flat_map { |path| read_entries(path) }
      assert_equal [{"row" => 2}], entries.map { |entry| entry.fetch("payload") }
    end
  end

  def test_writer_close_removes_empty_active_log
    with_writer do |writer, root|
      writer.send(:prepare_writer)

      assert_equal 1, log_files(root).length
      assert File.zero?(log_files(root).fetch(0))

      writer.close

      assert_empty log_files(root)
    end
  end

  def test_claimer_can_claim_an_old_bucket_while_the_writer_is_idle
    current_time = Time.utc(2026, 5, 6, 10, 15, 30)

    with_writer(clock: -> { current_time }) do |writer, root|
      writer.append("row" => 1)

      current_time = Time.utc(2026, 5, 6, 10, 16, 0)

      claim = claimer(root, current_time).claim_completed.fetch(0)
      assert_equal [{"row" => 1}], claim.read_lines.map { |line| JSON.parse(line).fetch("payload") }

      writer.append("row" => 2)

      assert_equal 1, log_files(root).length
      assert_match(/\A202605061016-/, File.basename(log_files(root).fetch(0)))
    ensure
      claim&.delete
    end
  end

  def test_writer_serializes_concurrent_appends_inside_one_process
    with_writer(sync: :none) do |writer, root|
      threads = 8.times.map do |thread_index|
        Thread.new do
          50.times do |row_index|
            writer.append("thread" => thread_index, "row" => row_index)
          end
        end
      end

      threads.each(&:join)

      entries = log_files(root).flat_map { |path| read_entries(path) }
      ids = entries.map { |entry| entry.fetch("id") }

      assert_equal 400, entries.length
      assert_equal ids.length, ids.uniq.length
      assert(entries.all? { |entry| entry.key?("payload") })
    end
  end

  def test_forked_children_open_their_own_writer
    skip "fork is unavailable on this Ruby" unless Process.respond_to?(:fork)

    with_writer(sync: :none) do |writer, root|
      writer.append("process" => "parent")

      pid = Process.fork do
        writer.append("process" => "child")
        writer.close
      end

      Process.wait(pid)
      writer.close

      entries = log_files(root).flat_map { |path| read_entries(path) }
      log_names = log_files(root).map { |path| File.basename(path) }

      assert_equal %w[child parent], entries.map { |entry| entry.fetch("payload").fetch("process") }.sort
      assert_equal 2, log_names.length
      assert(log_names.any? { |name| name.include?("-#{pid}-") })
      assert(log_names.any? { |name| name.include?("-#{Process.pid}-") })
    end
  end

  def test_writer_rejects_unknown_sync_modes
    error = assert_raises(ArgumentError) do
      TimeBucketStream::Writer.new(path: Dir.mktmpdir, sync: :sometimes)
    end

    assert_match(/unsupported file sync mode/, error.message)
  end

  def test_stream_uses_the_configured_codec
    current_time = Time.utc(2026, 5, 6, 10, 15, 30)
    codec = PrefixCodec.new

    with_stream(clock: -> { current_time }, codec: codec) do |stream, root|
      stream.append("row" => 1)

      line = File.read(log_files(root).fetch(0))
      assert_match(/\Acustom:/, line)

      current_time = Time.utc(2026, 5, 6, 10, 16, 0)
      batch = stream.read

      assert_equal [{"row" => 1}], batch.to_a
      assert_equal 1, codec.dumped.length
      assert_equal 1, codec.loaded.length
    end
  end

  def test_writer_rejects_codecs_that_dump_multiple_lines
    error = assert_raises(ArgumentError) do
      with_stream(codec: MultilineCodec.new) do |stream|
        stream.append("row" => 1)
      end
    end

    assert_match(/one line/, error.message)
  end

  def test_stream_rejects_invalid_codecs
    error = assert_raises(ArgumentError) do
      TimeBucketStream.new(path: Dir.mktmpdir, codec: Object.new)
    end

    assert_match(/dump and load/, error.message)
  end

  def test_stream_rejects_unknown_malformed_entry_modes
    error = assert_raises(ArgumentError) do
      TimeBucketStream.new(path: Dir.mktmpdir, malformed_entry: :maybe)
    end

    assert_match(/malformed_entry/, error.message)
  end

  def test_oj_codec_is_optional
    if oj_available?
      codec = TimeBucketStream::Codecs::Oj.new
      payload = {"id" => 1, "payload" => {"row" => 1}}

      assert_equal payload, codec.load(codec.dump(payload))
    else
      error = assert_raises(LoadError) do
        TimeBucketStream::Codecs::Oj.new
      end

      assert_match(/requires `gem "oj"`/, error.message)
    end
  end

  def test_claimer_moves_completed_past_files_to_processing
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      old_log = write_log(root, "202605061015-host-123-deadbeef.jsonl", [{"id" => 1, "payload" => {"row" => 1}}])
      write_log(root, "202605061016-host-123-cafebabe.jsonl", [{"id" => 2, "payload" => {"row" => 2}}])

      claim = claimer(root, now).claim_completed.fetch(0)

      assert_equal File.basename(old_log), claim.name
      assert_equal [{"id" => 1, "payload" => {"row" => 1}}], claim.read_lines.map { |line| JSON.parse(line) }
      refute File.exist?(old_log)
      assert File.exist?(File.join(processing_path(root), claim.name))
      assert_empty claimer(root, now).claim_completed
    ensure
      claim&.release
    end
  end

  def test_claimer_waits_for_the_default_claim_grace
    Dir.mktmpdir do |root|
      log_name = "202605061015-host-123-deadbeef.jsonl"
      write_log(root, log_name, [{"id" => 1, "payload" => {"row" => 1}}])

      early = Time.utc(2026, 5, 6, 10, 16, 5)
      ready = Time.utc(2026, 5, 6, 10, 16, 10)

      assert_empty TimeBucketStream::Claimer.new(
        path: stream_path(root),
        clock: -> { early }
      ).claim_completed
      claim = TimeBucketStream::Claimer.new(
        path: stream_path(root),
        clock: -> { ready }
      ).claim_completed.fetch(0)

      assert_equal log_name, claim.name
    ensure
      claim&.delete
    end
  end

  def test_claimer_rejects_invalid_claim_grace_values
    Dir.mktmpdir do |root|
      assert_raises(ArgumentError) do
        TimeBucketStream::Claimer.new(path: stream_path(root), claim_grace: -1)
      end

      assert_raises(ArgumentError) do
        TimeBucketStream::Claimer.new(path: stream_path(root), claim_grace: "soon")
      end
    end
  end

  def test_claimer_rejects_invalid_stale_partial_values
    Dir.mktmpdir do |root|
      assert_raises(ArgumentError) do
        TimeBucketStream::Claimer.new(path: stream_path(root), stale_partial_after: -1)
      end

      assert_raises(ArgumentError) do
        TimeBucketStream::Claimer.new(path: stream_path(root), stale_partial_after: "later")
      end
    end
  end

  def test_claimer_rejects_invalid_quarantine_retention_values
    Dir.mktmpdir do |root|
      assert_raises(ArgumentError) do
        TimeBucketStream::Claimer.new(path: stream_path(root), quarantine_retention: -1)
      end

      assert_raises(ArgumentError) do
        TimeBucketStream::Claimer.new(path: stream_path(root), quarantine_retention: "later")
      end

      assert_raises(ArgumentError) do
        TimeBucketStream::Claimer.new(path: stream_path(root), quarantine_retention: false)
      end
    end
  end

  def test_claimer_ignores_invalid_log_names
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      valid_log = write_log(root, "202605061015-host-123-deadbeef.jsonl", [{"id" => 1, "payload" => {"row" => 1}}])
      write_log(root, "202605061015-host-abc-cafebabe.jsonl", [{"id" => 2, "payload" => {"row" => 2}}])
      write_log(root, "202605061015-host-123-nothexid.jsonl", [{"id" => 3, "payload" => {"row" => 3}}])

      claim = claimer(root, now).claim_completed.fetch(0)

      assert_equal File.basename(valid_log), claim.name
      assert_empty claimer(root, now).claim_completed
    ensure
      claim&.delete
    end
  end

  def test_claimer_does_not_claim_partial_trailing_files
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      log_name = "202605061015-host-123-deadbeef.jsonl"
      write_log(root, log_name, [{"id" => "1", "payload" => {"row" => 1}}], newline: false)

      assert_empty claimer(root, now).claim_completed
      assert File.exist?(File.join(logs_path(root), log_name))
    end
  end

  def test_claimer_keeps_claim_locks_until_release
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      log_name = "202605061015-host-123-deadbeef.jsonl"
      write_log(root, log_name, [{"id" => "1", "payload" => {"row" => 1}}])

      first_claim = claimer(root, now).claim_completed.fetch(0)
      assert_empty claimer(root, now).claim_completed

      first_claim.release

      second_claim = claimer(root, now).claim_completed.fetch(0)
      assert_equal log_name, second_claim.name
      second_claim.release
    end
  end

  def test_concurrent_claimers_cannot_claim_the_same_file
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      write_log(root, "202605061015-host-123-deadbeef.jsonl", [{"id" => "1", "payload" => {"row" => 1}}])
      ready = Queue.new
      release = Queue.new
      results = Queue.new

      threads = 2.times.map do
        Thread.new do
          claims = claimer(root, now).claim_completed
          results << claims.map(&:name)
          ready << true
          release.pop if claims.any?
          claims.each(&:release)
        end
      end

      2.times { ready.pop }

      claimed_names = 2.times.map { results.pop }
      assert_equal [["202605061015-host-123-deadbeef.jsonl"], []], claimed_names.sort_by(&:length).reverse
    ensure
      release << true
      threads&.each(&:join)
    end
  end

  def test_claim_delete_removes_processing_file
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      log_name = "202605061015-host-123-deadbeef.jsonl"
      write_log(root, log_name, [{"id" => "1", "payload" => {"row" => 1}}])

      claim = claimer(root, now).claim_completed.fetch(0)

      assert File.exist?(claim.path)
      assert File.exist?(claim_lock_path(root, log_name))

      claim.delete

      refute File.exist?(claim.path)
      refute File.exist?(claim_lock_path(root, log_name))
      assert_empty claimer(root, now).claim_completed
    end
  end

  def test_claimer_delete_claimed_can_clean_up_a_released_processing_file
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      log_name = "202605061015-host-123-deadbeef.jsonl"
      write_log(root, log_name, [{"id" => "1", "payload" => {"row" => 1}}])

      claim = claimer(root, now).claim_completed.fetch(0)
      claim.release

      claimer(root, now).delete_claimed(log_name)

      refute File.exist?(File.join(processing_path(root), log_name))
    end
  end

  def test_stream_reads_claimed_entries_and_deletes_claimed_files
    current_time = Time.utc(2026, 5, 6, 10, 15, 30)

    with_stream(clock: -> { current_time }) do |stream, root|
      stream.append("row" => 1)
      current_time = Time.utc(2026, 5, 6, 10, 16, 0)

      batch = stream.read

      assert_kind_of Enumerable, batch
      assert_equal 1, batch.length
      assert_equal [{"row" => 1}], batch.to_a
      assert_match(/\A202605061015-.+\.jsonl:1\z/, batch.entries.first.first)
      assert_equal({"row" => 1}, batch.entries.first.last)
      assert_empty log_files(root)
      assert_equal 1, processing_files(root).length

      batch.delete

      assert_empty processing_files(root)
      assert batch.finished?
      refute batch.release
    end
  end

  def test_stream_drain_yields_payloads_and_deletes_claimed_files
    current_time = Time.utc(2026, 5, 6, 10, 15, 30)

    with_stream(clock: -> { current_time }) do |stream, root|
      stream.append("row" => 1)
      stream.append("row" => 2)
      current_time = Time.utc(2026, 5, 6, 10, 16, 0)

      payloads = []
      batch = stream.drain { |payload| payloads << payload }

      assert_equal [{"row" => 1}, {"row" => 2}], payloads
      assert_equal payloads, batch.to_a
      assert batch.finished?
      assert_empty log_files(root)
      assert_empty processing_files(root)
      assert_empty stream.read
    end
  end

  def test_stream_drain_releases_claimed_files_when_processing_raises
    current_time = Time.utc(2026, 5, 6, 10, 15, 30)

    with_stream(clock: -> { current_time }) do |stream, root|
      stream.append("row" => 1)
      stream.append("row" => 2)
      stream.append("row" => 3)
      current_time = Time.utc(2026, 5, 6, 10, 16, 0)

      payloads = []
      error = assert_raises(RuntimeError) do
        stream.drain do |payload|
          payloads << payload
          raise "stop" if payload.fetch("row") == 2
        end
      end

      assert_equal "stop", error.message
      assert_equal [{"row" => 1}, {"row" => 2}], payloads
      assert_equal 1, processing_files(root).length

      retry_batch = stream.read

      assert_equal [{"row" => 1}, {"row" => 2}, {"row" => 3}], retry_batch.to_a
      retry_batch.delete
      assert_empty processing_files(root)
    end
  end

  def test_stream_drain_requires_a_block
    with_stream do |stream|
      error = assert_raises(ArgumentError) do
        stream.drain
      end

      assert_equal "drain requires a block", error.message
    end
  end

  def test_stream_honors_the_default_claim_grace
    current_time = Time.utc(2026, 5, 6, 10, 15, 50)

    with_stream(clock: -> { current_time }, claim_grace: TimeBucketStream::DEFAULT_CLAIM_GRACE) do |stream, root|
      stream.append("row" => 1)

      current_time = Time.utc(2026, 5, 6, 10, 16, 5)

      assert_empty stream.read
      assert_equal 1, log_files(root).length

      current_time = Time.utc(2026, 5, 6, 10, 16, 10)

      assert_equal [{"row" => 1}], stream.read.to_a
    end
  end

  def test_stream_releases_claims_for_retry
    current_time = Time.utc(2026, 5, 6, 10, 15, 30)

    Dir.mktmpdir do |root|
      first_stream = TimeBucketStream.new(path: stream_path(root), clock: -> { current_time }, claim_grace: 0)
      first_stream.append("row" => 1)

      current_time = Time.utc(2026, 5, 6, 10, 16, 0)
      first_batch = first_stream.read
      first_batch.release

      second_stream = TimeBucketStream.new(path: stream_path(root), clock: -> { current_time }, claim_grace: 0)
      second_batch = second_stream.read

      assert_equal first_batch.entries, second_batch.entries
    ensure
      first_stream&.close
      second_stream&.close
    end
  end

  def test_stream_rereads_active_claims_without_reclaiming
    current_time = Time.utc(2026, 5, 6, 10, 15, 30)

    with_stream(clock: -> { current_time }) do |stream|
      stream.append("row" => 1)
      current_time = Time.utc(2026, 5, 6, 10, 16, 0)

      first_batch = stream.read
      second_batch = stream.read

      assert_equal first_batch.entries, second_batch.entries

      first_batch.delete
      assert_empty stream.read
    end
  end

  def test_stream_can_reclaim_a_processing_file_after_a_process_exits
    skip "fork is unavailable on this Ruby" unless Process.respond_to?(:fork)

    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      log_name = "202605061015-host-123-deadbeef.jsonl"
      write_log(root, log_name, [{"id" => 1, "payload" => {"row" => 1}}])
      read_pipe, write_pipe = IO.pipe

      pid = Process.fork do
        read_pipe.close

        stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, claim_grace: 0)
        write_pipe.write(JSON.generate(stream.read.entries))
        write_pipe.close
        exit! 0
      rescue => error
        write_pipe.write(JSON.generate("error" => error.full_message))
        write_pipe.close
        exit! 1
      end

      write_pipe.close
      child_result = JSON.parse(read_pipe.read)
      _, status = Process.wait2(pid)

      flunk child_result.fetch("error") if child_result.is_a?(Hash)
      assert status.success?
      assert_equal [["#{log_name}:1", {"row" => 1}]], child_result

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, claim_grace: 0)
      batch = stream.read

      assert_equal child_result, batch.entries

      batch.delete
      assert_empty processing_files(root)
    ensure
      read_pipe&.close unless read_pipe&.closed?
      write_pipe&.close unless write_pipe&.closed?
      stream&.close
    end
  end

  def test_stream_returns_malformed_and_empty_files_for_cleanup
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      FileUtils.mkdir_p(logs_path(root))
      malformed_name = "202605061015-host-123-badbad00.jsonl"
      empty_name = "202605061015-host-123-e1111111.jsonl"

      File.write(File.join(logs_path(root), malformed_name), "not-json\n")
      FileUtils.touch(File.join(logs_path(root), empty_name))

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, claim_grace: 0)
      batch = stream.read

      assert_empty batch
      assert_empty processing_files(root)
      assert_empty log_files(root)
      assert_empty claim_lock_files(root)

      metadata = quarantine_metadata(root)

      assert_equal ["empty_file", "malformed_jsonl"], metadata.map { |entry| entry.fetch("reason") }.sort
      assert_equal [empty_name, malformed_name].sort, metadata.map { |entry| entry.fetch("original_name") }.sort
    ensure
      stream&.close
    end
  end

  def test_stream_quarantines_entries_with_invalid_ids
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      FileUtils.mkdir_p(logs_path(root))
      invalid_id_name = "202605061015-host-123-badbad00.jsonl"

      File.write(File.join(logs_path(root), invalid_id_name), JSON.generate(
        "id" => "entry-1",
        "payload" => {"row" => 1}
      ) + "\n")

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, claim_grace: 0)
      batch = stream.read

      assert_empty batch
      assert_empty log_files(root)

      metadata = quarantine_metadata(root).fetch(0)
      assert_equal "malformed_jsonl", metadata.fetch("reason")
      assert_equal invalid_id_name, metadata.fetch("original_name")
    ensure
      stream&.close
    end
  end

  def test_stream_quarantines_the_whole_file_by_default_when_one_entry_is_malformed
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      log_name = "202605061015-host-123-deadbeef.jsonl"
      write_raw_log(root, log_name, [
        JSON.generate("id" => 1, "payload" => {"row" => 1}),
        "not-json"
      ])

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, claim_grace: 0)

      assert_empty stream.read
      assert_empty log_files(root)
      assert_empty processing_files(root)

      metadata = quarantine_metadata(root).fetch(0)
      assert_equal "malformed_jsonl", metadata.fetch("reason")
      assert_equal log_name, metadata.fetch("original_name")
    ensure
      stream&.close
    end
  end

  def test_stream_can_skip_malformed_entries
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      log_name = "202605061015-host-123-deadbeef.jsonl"
      write_raw_log(root, log_name, [
        JSON.generate("id" => 1, "payload" => {"row" => 1}),
        "not-json",
        JSON.generate("id" => "bad", "payload" => {"row" => 2}),
        JSON.generate("id" => 3, "payload" => {"row" => 3})
      ])

      stream = TimeBucketStream.new(
        path: stream_path(root),
        clock: -> { now },
        claim_grace: 0,
        malformed_entry: :skip
      )
      batch = stream.read

      assert_equal [{"row" => 1}, {"row" => 3}], batch.to_a
      assert_empty quarantine_files(root)

      batch.delete

      assert_empty log_files(root)
      assert_empty processing_files(root)
    ensure
      stream&.close
    end
  end

  def test_stream_deletes_all_malformed_claims_when_skipping_entries
    now = Time.utc(2026, 5, 6, 10, 16)

    Dir.mktmpdir do |root|
      write_raw_log(root, "202605061015-host-123-deadbeef.jsonl", [
        "not-json",
        JSON.generate("id" => "bad", "payload" => {"row" => 1})
      ])

      stream = TimeBucketStream.new(
        path: stream_path(root),
        clock: -> { now },
        claim_grace: 0,
        malformed_entry: :skip
      )

      assert_empty stream.read
      assert_empty log_files(root)
      assert_empty processing_files(root)
      assert_empty quarantine_files(root)
      assert_empty claim_lock_files(root)
      assert_empty stream.read
    ensure
      stream&.close
    end
  end

  def test_stream_automatically_quarantines_stale_partial_logs
    now = Time.utc(2026, 5, 6, 10, 30)

    Dir.mktmpdir do |root|
      stale_name = "202605061015-host-123-deadbeef.jsonl"
      write_log(root, stale_name, [{"id" => 1, "payload" => {"row" => 1}}], newline: false)

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, claim_grace: 0)

      assert_empty stream.read
      assert_empty log_files(root)
      assert_empty processing_files(root)

      metadata = quarantine_metadata(root).fetch(0)

      assert_equal "partial_trailing_line", metadata.fetch("reason")
      assert_equal stale_name, metadata.fetch("original_name")
      assert_equal 600, metadata.fetch("metadata").fetch("stale_partial_after")
    ensure
      stream&.close
    end
  end

  def test_stream_leaves_recent_partial_logs_for_later
    now = Time.utc(2026, 5, 6, 10, 30)

    Dir.mktmpdir do |root|
      recent_name = "202605061025-host-123-deadbeef.jsonl"
      write_log(root, recent_name, [{"id" => 1, "payload" => {"row" => 1}}], newline: false)

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, claim_grace: 0)

      assert_empty stream.read
      assert_equal [recent_name], log_files(root).map { |path| File.basename(path) }
      assert_empty quarantine_files(root)
    ensure
      stream&.close
    end
  end

  def test_stream_deletes_quarantine_files_older_than_retention
    now = Time.utc(2026, 5, 6, 12, 0)

    Dir.mktmpdir do |root|
      old_log, old_metadata = write_quarantine(
        root,
        quarantine_name(now - 7200),
        quarantined_at: now - 7200
      )
      fresh_log, fresh_metadata = write_quarantine(
        root,
        quarantine_name(now - 120),
        quarantined_at: now - 120
      )

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, quarantine_retention: 3600)
      stream.read

      refute File.exist?(old_log)
      refute File.exist?(old_metadata)
      assert File.exist?(fresh_log)
      assert File.exist?(fresh_metadata)
    ensure
      stream&.close
    end
  end

  def test_stream_uses_quarantine_filename_time_when_metadata_is_missing
    now = Time.utc(2026, 5, 6, 12, 0)

    Dir.mktmpdir do |root|
      old_log, = write_quarantine(root, quarantine_name(now - 7200), metadata: false)
      fresh_log, = write_quarantine(root, quarantine_name(now - 120), metadata: false)
      old_mtime = now - 7200

      File.utime(old_mtime, old_mtime, old_log)
      File.utime(old_mtime, old_mtime, fresh_log)

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, quarantine_retention: 3600)
      stream.read

      refute File.exist?(old_log)
      assert File.exist?(fresh_log)
    ensure
      stream&.close
    end
  end

  def test_stream_deletes_expired_orphan_quarantine_metadata
    now = Time.utc(2026, 5, 6, 12, 0)

    Dir.mktmpdir do |root|
      old_metadata = write_orphan_quarantine_metadata(root, quarantine_name(now - 7200), quarantined_at: now - 7200)
      fresh_metadata = write_orphan_quarantine_metadata(root, quarantine_name(now - 120), quarantined_at: now - 120)

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, quarantine_retention: 3600)
      stream.read

      refute File.exist?(old_metadata)
      assert File.exist?(fresh_metadata)
    ensure
      stream&.close
    end
  end

  def test_stream_uses_metadata_mtime_for_unparseable_orphan_quarantine_metadata
    now = Time.utc(2026, 5, 6, 12, 0)

    Dir.mktmpdir do |root|
      FileUtils.mkdir_p(quarantine_path(root))

      old_metadata = File.join(quarantine_path(root), "manual-bad-file.meta.json")
      fresh_metadata = File.join(quarantine_path(root), "manual-fresh-file.meta.json")

      File.write(old_metadata, "{")
      File.write(fresh_metadata, "{")

      old_time = now - 7200
      fresh_time = now - 120
      File.utime(old_time, old_time, old_metadata)
      File.utime(fresh_time, fresh_time, fresh_metadata)

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, quarantine_retention: 3600)
      stream.read

      refute File.exist?(old_metadata)
      assert File.exist?(fresh_metadata)
    ensure
      stream&.close
    end
  end

  def test_stream_keeps_quarantine_files_when_retention_is_nil
    now = Time.utc(2026, 5, 6, 12, 0)

    Dir.mktmpdir do |root|
      old_log, old_metadata = write_quarantine(
        root,
        quarantine_name(now - 7200),
        quarantined_at: now - 7200
      )

      stream = TimeBucketStream.new(path: stream_path(root), clock: -> { now }, quarantine_retention: nil)
      stream.read

      assert File.exist?(old_log)
      assert File.exist?(old_metadata)
    ensure
      stream&.close
    end
  end

  private

  def with_stream(sync: :flush, clock: Time, claim_grace: 0, quarantine_retention: TimeBucketStream::DEFAULT_QUARANTINE_RETENTION, codec: TimeBucketStream::Codecs::Json.new)
    Dir.mktmpdir do |root|
      stream = TimeBucketStream.new(
        path: stream_path(root),
        sync: sync,
        clock: clock,
        claim_grace: claim_grace,
        quarantine_retention: quarantine_retention,
        codec: codec
      )
      yield stream, root
    ensure
      stream&.close
    end
  end

  def with_writer(sync: :flush, clock: Time)
    Dir.mktmpdir do |root|
      writer = TimeBucketStream::Writer.new(path: stream_path(root), sync: sync, clock: clock)
      yield writer, root
    ensure
      writer&.close
    end
  end

  def claimer(root, time, claim_grace: 0)
    TimeBucketStream::Claimer.new(path: stream_path(root), clock: -> { time }, claim_grace: claim_grace)
  end

  def stream_path(root)
    root
  end

  def logs_path(root)
    File.join(stream_path(root), "logs")
  end

  def processing_path(root)
    File.join(stream_path(root), "processing")
  end

  def quarantine_path(root)
    File.join(stream_path(root), "quarantine")
  end

  def claim_locks_path(root)
    File.join(stream_path(root), "claim_locks")
  end

  def processing_files(root)
    Dir[File.join(processing_path(root), "*.jsonl")].sort
  end

  def claim_lock_path(root, log_name)
    File.join(claim_locks_path(root), "#{log_name}.lock")
  end

  def claim_lock_files(root)
    Dir[File.join(claim_locks_path(root), "*.lock")].sort
  end

  def quarantine_files(root)
    Dir[File.join(quarantine_path(root), "*.jsonl")].reject { |path| path.end_with?(".meta.json") }.sort
  end

  def quarantine_metadata(root)
    Dir[File.join(quarantine_path(root), "*.meta.json")].sort.map do |path|
      JSON.parse(File.read(path))
    end
  end

  def log_files(root)
    Dir[File.join(logs_path(root), "*.jsonl")].sort
  end

  def write_log(root, log_name, entries, newline: true)
    FileUtils.mkdir_p(logs_path(root))
    path = File.join(logs_path(root), log_name)
    data = entries.map { |entry| JSON.generate(entry) }.join("\n")
    data = "#{data}\n" if newline
    File.write(path, data)
    path
  end

  def write_raw_log(root, log_name, lines)
    FileUtils.mkdir_p(logs_path(root))
    path = File.join(logs_path(root), log_name)
    File.write(path, "#{lines.join("\n")}\n")
    path
  end

  def write_quarantine(root, log_name, quarantined_at: Time.now.utc, metadata: true)
    FileUtils.mkdir_p(quarantine_path(root))

    log_path = File.join(quarantine_path(root), log_name)
    metadata_path = "#{log_path}.meta.json"

    File.write(log_path, "bad\n")
    File.write(metadata_path, JSON.pretty_generate("quarantined_at" => quarantined_at.utc.iso8601(6))) if metadata

    [log_path, metadata_path]
  end

  def write_orphan_quarantine_metadata(root, log_name, quarantined_at:)
    _, metadata_path = write_quarantine(root, log_name, quarantined_at: quarantined_at)
    FileUtils.rm_f(File.join(quarantine_path(root), log_name))
    metadata_path
  end

  def quarantine_name(time)
    "202605061015-host-123-deadbeef.q-#{time.utc.strftime("%Y%m%d%H%M%S%6N")}-123-cafe1234.jsonl"
  end

  def read_entries(path)
    File.readlines(path, chomp: true).map { |line| JSON.parse(line) }
  end

  def oj_available?
    require "oj"
    true
  rescue LoadError
    false
  end

  class PrefixCodec
    attr_reader :dumped, :loaded

    def initialize
      @dumped = []
      @loaded = []
    end

    def dump(value)
      dumped << value
      "custom:#{JSON.generate(value)}"
    end

    def load(value)
      loaded << value
      JSON.parse(value.delete_prefix("custom:"))
    end
  end

  class MultilineCodec
    def dump(_value)
      "line one\nline two"
    end

    def load(_value)
      nil
    end
  end
end
