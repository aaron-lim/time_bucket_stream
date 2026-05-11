# frozen_string_literal: true

class TimeBucketStream
  class Paths
    attr_reader :path

    def initialize(path:)
      @path = normalize_path(path)
    end

    def base
      path
    end

    def logs
      @logs ||= File.join(base, "logs")
    end

    def processing
      @processing ||= File.join(base, "processing")
    end

    def quarantine
      @quarantine ||= File.join(base, "quarantine")
    end

    def claim_locks
      @claim_locks ||= File.join(base, "claim_locks")
    end

    def log_for(writer_id)
      File.join(logs, "#{writer_id}.jsonl")
    end

    def processing_log_for(log_name)
      File.join(processing, log_name)
    end

    def quarantine_log_for(log_name)
      File.join(quarantine, log_name)
    end

    def quarantine_metadata_for(log_name)
      File.join(quarantine, "#{log_name}.meta.json")
    end

    def claim_lock_for(log_name)
      File.join(claim_locks, "#{log_name}.lock")
    end

    private

    def normalize_path(path)
      value = path.to_s
      raise ArgumentError, "path must not be blank" if value.strip.empty?

      File.expand_path(value)
    end
  end
end
