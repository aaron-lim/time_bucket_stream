# frozen_string_literal: true

require_relative "lib/time_bucket_stream/version"

Gem::Specification.new do |spec|
  spec.name = "time_bucket_stream"
  spec.version = TimeBucketStream::VERSION
  spec.authors = ["Lim Yu Kwang"]
  spec.email = ["aaron.lim.yu.kwang@gmail.com"]

  spec.summary = "Time-bucketed file streams for Ruby."
  spec.description = "TimeBucketStream appends JSONL events into time-bucketed files and atomically claims completed files for processing."
  spec.homepage = "https://github.com/aaron-lim/time_bucket_stream"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["source_code_uri"] = spec.homepage

  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
end
