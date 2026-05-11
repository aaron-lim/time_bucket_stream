# frozen_string_literal: true

require "fileutils"
require "json"
require "securerandom"
require "time"

class TimeBucketStream
  class Claim
    attr_reader :name, :path

    def initialize(name:, path:, lock_file:)
      @name = name
      @path = path
      @lock_file = lock_file
      @lock_path = lock_file.path
    end

    def read_lines
      File.readlines(path, chomp: true)
    rescue SystemCallError, IOError
      []
    end

    def delete
      finished = false
      FileUtils.rm_f(path)
      finished = true
    ensure
      finished ? finish : release
    end

    def quarantine(paths:, reason:, metadata: {}, clock: Time)
      finished = false
      quarantine_name = quarantine_name_for(current_time(clock))
      quarantine_path = paths.quarantine_log_for(quarantine_name)
      metadata_path = paths.quarantine_metadata_for(quarantine_name)

      FileUtils.mkdir_p(paths.quarantine)
      File.rename(path, quarantine_path)
      finished = true
      write_metadata(metadata_path, quarantine_metadata(
        quarantine_name: quarantine_name,
        quarantine_path: quarantine_path,
        reason: reason,
        metadata: metadata,
        clock: clock
      ))
      quarantine_path
    ensure
      finished ? finish : release
    end

    def release
      unlock_file(@lock_file)
      close_file(@lock_file)
      @lock_file = nil
    end

    private

    def finish
      lock_path = @lock_path

      release
      FileUtils.rm_f(lock_path) if lock_path
    rescue SystemCallError, IOError
      nil
    end

    def quarantine_name_for(time)
      base_name = name.delete_suffix(".jsonl")
      suffix = [
        "q",
        time.utc.strftime("%Y%m%d%H%M%S%6N"),
        Process.pid,
        SecureRandom.hex(4)
      ].join("-")

      "#{base_name}.#{suffix}.jsonl"
    end

    def quarantine_metadata(quarantine_name:, quarantine_path:, reason:, metadata:, clock:)
      {
        "reason" => normalized_reason(reason),
        "original_name" => name,
        "original_path" => path,
        "quarantine_name" => quarantine_name,
        "quarantine_path" => quarantine_path,
        "quarantined_at" => current_time(clock).utc.iso8601(6),
        "pid" => Process.pid,
        "metadata" => json_safe(metadata)
      }
    end

    def normalized_reason(reason)
      value = reason.to_s
      value.empty? ? "unspecified" : value
    end

    def write_metadata(path, payload)
      tmp_path = "#{path}.#{Process.pid}.#{SecureRandom.hex(4)}.tmp"

      File.write(tmp_path, JSON.pretty_generate(payload))
      File.rename(tmp_path, path)
    rescue JSON::GeneratorError, SystemCallError, IOError
      FileUtils.rm_f(tmp_path)
      nil
    end

    def json_safe(value)
      JSON.parse(JSON.generate(value))
    rescue JSON::GeneratorError, JSON::ParserError, TypeError
      value.inspect
    end

    def current_time(clock)
      clock.respond_to?(:call) ? clock.call : clock.now
    end

    def unlock_file(file)
      file&.flock(File::LOCK_UN)
    rescue SystemCallError, IOError
      nil
    end

    def close_file(file)
      file&.close
    rescue SystemCallError, IOError
      nil
    end
  end
end
