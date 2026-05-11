# frozen_string_literal: true

class TimeBucketStream
  module Codecs
    module_function

    def validate!(codec)
      return codec if codec.respond_to?(:dump) && codec.respond_to?(:load)

      raise ArgumentError, "codec must respond to dump and load"
    end
  end
end
