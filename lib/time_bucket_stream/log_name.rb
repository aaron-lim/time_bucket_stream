# frozen_string_literal: true

class TimeBucketStream
  module LogName
    PATTERN = /\A(?<bucket>\d{12})-[A-Za-z0-9_.-]+-\d+-[0-9a-f]{8}\.jsonl\z/

    module_function

    def valid?(name)
      name = name.to_s
      name == File.basename(name) && PATTERN.match?(name)
    rescue ArgumentError, EncodingError
      false
    end

    def bucket(name)
      match = PATTERN.match(name.to_s)
      match && match[:bucket]
    rescue ArgumentError, EncodingError
      nil
    end
  end
end
