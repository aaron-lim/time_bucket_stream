# frozen_string_literal: true

class TimeBucketStream
  DEFAULT_CLAIM_GRACE = Claimer::DEFAULT_CLAIM_GRACE
  DEFAULT_STALE_PARTIAL_AFTER = Claimer::DEFAULT_STALE_PARTIAL_AFTER
  DEFAULT_QUARANTINE_RETENTION = Claimer::DEFAULT_QUARANTINE_RETENTION
  DEFAULT_MALFORMED_ENTRY = :quarantine
  MALFORMED_ENTRY_MODES = %i[quarantine skip].freeze

  attr_reader :claim_grace, :codec, :malformed_entry, :paths, :quarantine_retention, :stale_partial_after, :sync

  def initialize(path:, sync: :flush, clock: Time, claim_grace: DEFAULT_CLAIM_GRACE, stale_partial_after: DEFAULT_STALE_PARTIAL_AFTER, quarantine_retention: DEFAULT_QUARANTINE_RETENTION, malformed_entry: DEFAULT_MALFORMED_ENTRY, codec: Codecs::Json.new)
    @paths = Paths.new(path: path)
    @sync = sync.respond_to?(:to_sym) ? sync.to_sym : sync
    @clock = clock
    @codec = Codecs.validate!(codec)
    @claim_grace = Claimer.normalize_claim_grace(claim_grace)
    @stale_partial_after = Claimer.normalize_stale_partial_after(stale_partial_after)
    @quarantine_retention = Claimer.normalize_quarantine_retention(quarantine_retention)
    @malformed_entry = normalize_malformed_entry(malformed_entry)
    @claims_by_log_name = {}

    writer
    claimer
  end

  def append(payload)
    writer.append(payload)
  end

  def read
    writer.close_stale

    Batch.new(
      entries: active_claim_entries + new_claim_entries,
      on_delete: method(:delete_entries),
      on_release: method(:release_entries)
    )
  end

  def drain
    raise ArgumentError, "drain requires a block" unless block_given?

    batch = read
    completed = false

    batch.each { |payload| yield payload }
    batch.delete
    completed = true

    batch
  ensure
    batch.release if batch && !completed && !batch.finished?
  end

  def close
    release_claims
    writer.close
  end

  private

  def delete_entries(ids)
    log_names_from_entry_ids(ids).each { |log_name| delete_claim(log_name) }
  end

  def release_entries(ids)
    log_names_from_entry_ids(ids).each { |log_name| release_claim(log_name) }
  end

  def active_claim_entries
    @claims_by_log_name.values.flat_map do |claim|
      entries = read_claim(claim)
      next entries unless entries.empty?

      @claims_by_log_name.delete(claim.name)
      claim.release
      []
    end
  end

  def new_claim_entries
    claimer.claim_completed.flat_map do |claim|
      if @claims_by_log_name.key?(claim.name)
        claim.release
        next []
      end

      entries_from_claim(claim)
    end
  end

  def entries_from_claim(claim)
    entries = read_claim(claim)

    if entries.empty?
      claim.release
    else
      @claims_by_log_name[claim.name] = claim
    end

    entries
  end

  def read_claim(claim)
    lines = claim.read_lines
    return quarantine_claim(claim, reason: "empty_file", metadata: {"line_count" => 0}) if lines.empty? && empty_claim?(claim)
    return [] if lines.empty?

    entries = []
    malformed_lines = []

    lines.each_with_index do |line, index|
      entry = parse_entry(line)
      if entry
        entries << [
          encode_entry_id(claim.name, entry.fetch("id")),
          entry.fetch("payload")
        ]
      else
        malformed_lines << malformed_line_summary(line, index)
      end
    end

    return entries if malformed_lines.empty?

    if malformed_entry == :skip
      return entries if entries.any?

      return discard_fully_malformed_claim(claim)
    end

    quarantine_claim(
      claim,
      reason: "malformed_jsonl",
      metadata: {
        "line_count" => lines.length,
        "malformed_lines" => malformed_lines
      }
    )
  rescue SystemCallError, IOError
    []
  end

  def parse_entry(line)
    entry = codec.load(line)
    return unless valid_entry?(entry)

    entry
  rescue
    nil
  end

  def valid_entry?(entry)
    entry.is_a?(Hash) &&
      entry.fetch("id", nil).is_a?(Integer) &&
      entry.fetch("id").positive? &&
      entry.key?("payload")
  end

  def malformed_line_summary(line, index)
    {
      "line" => index + 1,
      "bytes" => line.bytesize,
      "sample" => line.byteslice(0, 200)
    }
  end

  def discard_fully_malformed_claim(claim)
    @claims_by_log_name.delete(claim.name)
    claim.delete
    []
  rescue SystemCallError, IOError
    claim.release
    []
  end

  def normalize_malformed_entry(value)
    mode = value.respond_to?(:to_sym) ? value.to_sym : value
    return mode if MALFORMED_ENTRY_MODES.include?(mode)

    raise ArgumentError, "malformed_entry must be :quarantine or :skip"
  end

  def encode_entry_id(log_name, entry_id)
    "#{log_name}:#{entry_id}"
  end

  def log_names_from_entry_ids(ids)
    Array(ids).filter_map { |id| decode_entry_id(id) }.uniq
  end

  def decode_entry_id(id)
    value = id.to_s
    separator = value.index(":")
    return unless separator

    log_name = value[0...separator]
    log_name if valid_log_name?(log_name)
  end

  def valid_log_name?(log_name)
    LogName.valid?(log_name)
  end

  def empty_claim?(claim)
    File.zero?(claim.path)
  rescue SystemCallError, IOError
    false
  end

  def delete_claim(log_name)
    claim = @claims_by_log_name.delete(log_name)
    claim ? claim.delete : claimer.delete_claimed(log_name)
  rescue SystemCallError, IOError
    release_claim(log_name)
  end

  def release_claim(log_name)
    claim = @claims_by_log_name.delete(log_name)
    claim ? claim.release : claimer.release_claimed(log_name)
  rescue SystemCallError, IOError
    nil
  end

  def quarantine_claim(claim, reason:, metadata:)
    @claims_by_log_name.delete(claim.name)
    claim.quarantine(paths: paths, reason: reason, metadata: metadata, clock: @clock)
    []
  rescue SystemCallError, IOError
    claim.release
    []
  end

  def release_claims
    claims = @claims_by_log_name.values
    @claims_by_log_name = {}
    claims.each(&:release)
  rescue SystemCallError, IOError
    nil
  end

  def writer
    @writer ||= Writer.new(
      path: paths.path,
      sync: sync,
      clock: @clock,
      codec: codec
    )
  end

  def claimer
    @claimer ||= Claimer.new(
      path: paths.path,
      clock: @clock,
      claim_grace: claim_grace,
      stale_partial_after: stale_partial_after,
      quarantine_retention: quarantine_retention
    )
  end
end
