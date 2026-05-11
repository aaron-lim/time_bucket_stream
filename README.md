# TimeBucketStream

TimeBucketStream writes machine-readable events into time-bucketed JSONL files.

It is built for fast appends and safe batch processing:

- writers append JSONL to per-process bucket files
- old completed files can be atomically claimed for processing
- claimed files can be deleted after successful processing or retried after a crash
- bad files are automatically quarantined with metadata instead of retried forever

The stream stays generic. It does not know about metrics, jobs, queues, or your
database.

## Installation

Install from GitHub while the gem is young:

```ruby
gem "time_bucket_stream", github: "aaron-lim/time_bucket_stream", branch: "main"
```

After a RubyGems release:

```bash
bundle add time_bucket_stream
```

## Usage

Use `TimeBucketStream.new` when you want a ready-to-process pending queue:

```ruby
stream = TimeBucketStream.new(path: "/tmp/my_app_events/sync")

stream.append(account_id: 123, status: "success")
```

The path is the stream directory. TimeBucketStream creates its own folders
inside it.

Use a different path for each independent stream you want to process
separately:

```ruby
sync_stream = TimeBucketStream.new(path: "/tmp/my_app_events/sync")
mail_stream = TimeBucketStream.new(path: "/tmp/my_app_events/mail")
```

Those streams do not share log files, processing files, locks, or quarantine
files.

Process completed old files with `drain`:

```ruby
stream.drain do |payload|
  process(payload)
end
```

On success, `drain` deletes the claimed files. If your block raises,
`drain` releases the batch and re-raises the error, so a later processor can
retry the same file.

Use `read` when you want manual batch control:

```ruby
batch = stream.read

batch.each do |payload|
  process(payload)
end

batch.delete
```

`batch` is `Enumerable`. It yields payloads:

```ruby
batch.map { |payload| payload.fetch("account_id") }
```

The file line stores only the row id inside that file:

```json
{"id":1,"payload":{"account_id":123,"status":"success"}}
```

If processing cannot finish, release the entries instead of deleting them:

```ruby
batch.release
```

That leaves the file in `processing/` so a later processor can claim it again.

The public API stays intentionally small:

- `TimeBucketStream.new(path:, ...)`
- `append(payload)`
- `drain { |payload| ... }`
- `read`
- `close`

The batch API is also small:

- `each`
- `empty?`
- `size`
- `delete`
- `release`

## File Layout

For `path: "/tmp/my_app_events/sync"`, files live under:

```text
/tmp/my_app_events/sync/
  logs/
    202605061530-host-12345-abcd1234.jsonl
  processing/
    202605061529-host-12345-deadbeef.jsonl
  quarantine/
    202605061520-host-12345-badbad00.q-20260506153100123456-6789-cafe1234.jsonl
    202605061520-host-12345-badbad00.q-20260506153100123456-6789-cafe1234.jsonl.meta.json
  claim_locks/
    202605061529-host-12345-deadbeef.jsonl.lock
```

`logs/` contains files that writers may still own. A valid log filename is:

```text
YYYYMMDDHHMM-host-pid-randomhex.jsonl
```

`processing/` contains files that a processor has claimed. The filename stays
the same; only the directory changes.

`quarantine/` contains files that should not be retried automatically. Each
quarantined file gets a `.meta.json` sidecar with the reason, original path,
quarantine time, process id, and diagnostic metadata. Old quarantine files are
cleaned up automatically according to `quarantine_retention`.

## Why Claiming Is Safe

Each writer creates one file per process per UTC minute.

The claimer only touches files whose filename bucket is old enough according to
`claim_grace`. With the default grace of 10 seconds, a `10:15` file becomes
claimable at `10:16:10`, not at `10:16:00`.

Before claiming a file, it checks that:

- the filename is valid
- the file ends with a newline
- the per-file claim lock can be acquired

Then it moves the file with `File.rename`:

```text
logs/<bucket>-<host>-<pid>-<random>.jsonl
  -> processing/<bucket>-<host>-<pid>-<random>.jsonl
```

That rename is atomic when both directories are on the same filesystem. If two
processors race, only one gets the claim lock and only one can move the file.

If a processor crashes after claiming, the operating system releases the claim
lock. A later processor can reclaim the file from `processing/`.

## Quarantine

Quarantine is automatic when using streams created by `TimeBucketStream.new`.

`read` moves these files to `quarantine/` and does not return entries from
them:

- completed empty files
- completed files with malformed JSONL or invalid entry shape
- partial trailing-line files older than `stale_partial_after`

`stale_partial_after` defaults to 600 seconds.

Recent partial files stay in `logs/` because a writer may still be alive. Locked
processing files stay untouched because another processor may still be active.

By default, one malformed entry quarantines the whole claimed file. If your app
prefers to skip bad entries and keep processing the good ones, use:

```ruby
stream = TimeBucketStream.new(
  path: "/tmp/my_app_events/sync",
  malformed_entry: :skip
)
```

With `malformed_entry: :skip`, malformed JSONL entries and entries with an
invalid shape are skipped. If every entry in a claimed file is malformed, the
file is deleted during `read` so it will not be retried forever.

Quarantine retention defaults to 7 days. Retention cleanup runs during `read`.
It deletes expired quarantined JSONL files with their metadata sidecars, and it
also removes expired orphan metadata files. Pass `quarantine_retention: nil` if
you want to keep quarantine files until your own cleanup removes them.

For multi-host processing on a shared directory, the filesystem must provide
correct `rename(2)` and `flock(2)` behavior. For the highest-confidence setup,
write streams to host-local storage and process each host's stream locally, or
use a shared filesystem whose locking semantics you have tested.

## Options

The constructor accepts these options:

| Option | Default | Effect |
| --- | --- | --- |
| `path:` | required | Stream directory. TimeBucketStream writes `logs/`, `processing/`, `quarantine/`, and `claim_locks/` inside this directory. |
| `sync:` | `:flush` | Controls how strongly each append is flushed to disk. See sync modes below. |
| `claim_grace:` | `10` | Seconds to wait after a UTC minute boundary before claiming that minute's completed files. A `10:15` file becomes claimable at `10:16:10` by default. |
| `stale_partial_after:` | `600` | Seconds to wait before quarantining an old file that does not end with a newline. Recent partial files are left alone because a writer may still be alive. |
| `malformed_entry:` | `:quarantine` | Controls malformed JSONL entries or invalid entry shapes. Use `:quarantine` to quarantine the whole file, or `:skip` to skip malformed entries and keep valid entries. |
| `quarantine_retention:` | `604800` | Seconds to keep quarantine files before automatic cleanup. Use `nil` to disable quarantine cleanup. |
| `codec:` | `TimeBucketStream::Codecs::Json.new` | Object used to dump and load one JSONL entry. Use this when your app wants a faster JSON library such as Oj. |

Example with the common options:

```ruby
stream = TimeBucketStream.new(
  path: "/tmp/my_app_events/sync",
  sync: :flush,
  claim_grace: 10,
  stale_partial_after: 600,
  malformed_entry: :quarantine,
  quarantine_retention: 7 * 24 * 60 * 60,
  codec: TimeBucketStream::Codecs::Json.new
)
```

### Sync Modes

```ruby
sync: :none  # fastest; OS buffers decide when bytes hit disk
sync: :flush # default; flush after every append
sync: :fsync # strongest; fsync after every append
```

Use `:flush` unless you have a measured reason to choose differently.

### Codecs

The default codec uses Ruby's stdlib `JSON`.

Oj is optional. TimeBucketStream does not depend on it unless your app chooses
to install it:

```ruby
gem "oj"
```

Then configure the stream:

```ruby
stream = TimeBucketStream.new(
  path: "/tmp/my_app_events/sync",
  codec: TimeBucketStream::Codecs::Oj.new
)
```

The Oj codec uses compat mode so it behaves like the stdlib JSON gem for normal
hashes, arrays, strings, numbers, booleans, and nil.

Custom codecs can be used too. A codec only needs `dump` and `load`:

```ruby
class MyCodec
  def dump(value)
    JSON.generate(value)
  end

  def load(value)
    JSON.parse(value)
  end
end
```

`dump` must return one line. If it returns a string containing a newline,
`append` raises, because one stream entry must fit on one JSONL line.

## Stress Testing

The normal test suite is fast and deterministic. The stress test is separate:

```bash
bundle exec rake stress
```

It starts many writer processes, intentionally crashes some processor processes
after they claim files, then drains the stream again and verifies:

- every written payload was processed
- no payload was processed twice
- no unexpected payload appeared
- no `logs/*.jsonl` or `processing/*.jsonl` files were left behind

The JSON report also includes write, crash-recovery, drain, and total
throughput numbers.

Useful knobs:

```bash
WRITERS=24 EVENTS_PER_WRITER=5000 PROCESSORS=12 CRASHERS=4 bundle exec rake stress
STREAM_PATH=/tmp/tbs-stress KEEP_PATH=1 bundle exec rake stress
SYNC=fsync bundle exec rake stress
CODEC=oj bundle exec rake stress
PAYLOAD_BYTES=4096 bundle exec rake stress
```

Use `STREAM_PATH` when you want to test a specific filesystem or mounted disk.
Use `PAYLOAD_BYTES` when you want to compare larger event rows.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/aaron-lim/time_bucket_stream.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
