# frozen_string_literal: true

require "bundler/gem_tasks"
require "minitest/test_task"

Minitest::TestTask.create

require "standard/rake"

namespace :stress do
  desc "Run the file-stream crash-recovery stress test"
  task :file_stream do
    ruby "test/stress/file_stream_stress.rb"
  end
end

desc "Run all stress tests"
task stress: "stress:file_stream"

task default: %i[test standard]
