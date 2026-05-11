# frozen_string_literal: true

require "fileutils"

class TimeBucketStream
  class Claimer
    DEFAULT_CLAIM_GRACE = 10
    DEFAULT_STALE_PARTIAL_AFTER = 600
    DEFAULT_QUARANTINE_RETENTION = Quarantine::DEFAULT_RETENTION

    attr_reader :claim_grace, :paths, :quarantine_retention, :stale_partial_after

    def self.normalize_claim_grace(value)
      seconds = Integer(value)
      return seconds if seconds >= 0

      raise ArgumentError
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "claim_grace must be a non-negative number of seconds"
    end

    def self.normalize_stale_partial_after(value)
      seconds = Integer(value)
      return seconds if seconds >= 0

      raise ArgumentError
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "stale_partial_after must be a non-negative number of seconds"
    end

    def self.normalize_quarantine_retention(value)
      Quarantine.normalize_retention(value)
    end

    def initialize(path:, clock: Time, claim_grace: DEFAULT_CLAIM_GRACE, stale_partial_after: DEFAULT_STALE_PARTIAL_AFTER, quarantine_retention: DEFAULT_QUARANTINE_RETENTION)
      @paths = Paths.new(path: path)
      @clock = clock
      @claim_grace = self.class.normalize_claim_grace(claim_grace)
      @stale_partial_after = self.class.normalize_stale_partial_after(stale_partial_after)
      @quarantine_retention = self.class.normalize_quarantine_retention(quarantine_retention)

      ensure_directories
    end

    def claim_completed(before: claim_before_bucket)
      before = before.to_s

      ensure_directories
      cleanup_quarantine
      quarantine_stale_partials

      claimable_log_names(before).filter_map do |log_name|
        claim_log(log_name, before)
      end
    end

    def delete_claimed(log_name)
      claim_processing(log_name)&.delete
    end

    def release_claimed(log_name)
      claim_processing(log_name)&.release
    end

    private

    def claimable_log_names(before)
      (processing_log_names + completed_log_names(before)).uniq.sort
    end

    def processing_log_names
      Dir.children(paths.processing).grep(/\.jsonl\z/).select { |name| valid_log_name?(name) }
    rescue SystemCallError
      []
    end

    def completed_log_names(before)
      Dir.children(paths.logs)
        .grep(/\.jsonl\z/)
        .select { |name| processable_log?(name, before) }
    rescue SystemCallError
      []
    end

    def partial_log_names(before)
      Dir.children(paths.logs)
        .grep(/\.jsonl\z/)
        .select { |name| partial_log?(name, before) }
    rescue SystemCallError
      []
    end

    def claim_log(log_name, before)
      return unless processable_log?(log_name, before)

      claim_processing(log_name) || claim_pending(log_name, before)
    end

    def claim_processing(log_name)
      return unless valid_log_name?(log_name)

      claim_path = paths.processing_log_for(log_name)

      with_claim_lock(log_name) do |lock_file|
        next unless File.exist?(claim_path)
        next unless complete_file?(claim_path)

        build_claim(log_name, claim_path, lock_file)
      end
    rescue SystemCallError, IOError
      nil
    end

    def quarantine_stale_partials
      before = stale_partial_before_bucket

      partial_log_names(before).each do |log_name|
        quarantine_partial(log_name, before)
      end
    end

    def quarantine_partial(log_name, before)
      return unless partial_log?(log_name, before)

      source_path = File.join(paths.logs, log_name)

      with_claim_lock(log_name) do |lock_file|
        next unless File.exist?(source_path)
        next unless partial_log?(log_name, before)

        Claim.new(name: log_name, path: source_path, lock_file: lock_file).quarantine(
          paths: paths,
          reason: "partial_trailing_line",
          metadata: {
            "stale_partial_after" => stale_partial_after,
            "before_bucket" => before
          },
          clock: @clock
        )
      end
    rescue SystemCallError, IOError
      nil
    end

    def claim_pending(log_name, before)
      return unless processable_log?(log_name, before)

      source_path = File.join(paths.logs, log_name)
      claim_path = paths.processing_log_for(log_name)

      with_claim_lock(log_name) do |lock_file|
        next if File.exist?(claim_path)
        next unless File.exist?(source_path)
        next unless processable_log?(log_name, before)
        next unless complete_file?(source_path)

        File.rename(source_path, claim_path)
        build_claim(log_name, claim_path, lock_file)
      end
    rescue SystemCallError, IOError
      nil
    end

    def build_claim(log_name, path, lock_file)
      Claim.new(
        name: log_name,
        path: path,
        lock_file: lock_file
      )
    end

    def with_claim_lock(log_name)
      FileUtils.mkdir_p(paths.claim_locks)

      file = File.open(paths.claim_lock_for(log_name), File::RDWR | File::CREAT, 0o600)
      return close_file(file) unless file.flock(File::LOCK_EX | File::LOCK_NB)

      claim = yield file
      return claim if claim

      unlock_file(file)
      close_file(file)
      cleanup_unused_claim_lock(log_name)
      nil
    rescue
      unlock_file(file)
      close_file(file)
      raise
    end

    def complete_file?(path)
      size = File.size(path)
      return true if size.zero?

      File.open(path, File::RDONLY) do |file|
        file.seek(size - 1)
        file.read(1) == "\n"
      end
    rescue SystemCallError, IOError
      false
    end

    def ensure_directories
      FileUtils.mkdir_p(paths.logs)
      FileUtils.mkdir_p(paths.processing)
      FileUtils.mkdir_p(paths.claim_locks)
    end

    def processable_log?(log_name, before)
      bucket = log_bucket(log_name)
      bucket && bucket < before
    end

    def partial_log?(log_name, before)
      return false unless processable_log?(log_name, before)

      !complete_file?(File.join(paths.logs, log_name))
    end

    def valid_log_name?(log_name)
      LogName.valid?(log_name)
    end

    def log_bucket(log_name)
      LogName.bucket(log_name)
    end

    def claim_before_bucket
      (current_time.utc - claim_grace).strftime(Writer::BUCKET_FORMAT)
    end

    def stale_partial_before_bucket
      (current_time.utc - stale_partial_after).strftime(Writer::BUCKET_FORMAT)
    end

    def current_time
      @clock.respond_to?(:call) ? @clock.call : @clock.now
    end

    def cleanup_quarantine
      quarantine.cleanup
    end

    def quarantine
      @quarantine ||= Quarantine.new(
        path: paths.path,
        clock: @clock,
        retention: quarantine_retention
      )
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

    def cleanup_unused_claim_lock(log_name)
      return if File.exist?(File.join(paths.logs, log_name))
      return if File.exist?(paths.processing_log_for(log_name))

      FileUtils.rm_f(paths.claim_lock_for(log_name))
    rescue SystemCallError, IOError
      nil
    end
  end
end
