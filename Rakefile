require 'thwait'
require 'optparse'
require_relative 'db_connection'
require_relative 'worker'

CONNECTION_OPTS = {
  host: 'localhost',
  port: 5432,
  user: 'timur',
  password: 'timur',
  dbname: 'test'
}.freeze

desc 'updates test table by setting uuid in multi-threaded way in forked process'
task :set_uuids do
  parsed_args = parse_args('set_uuids')
  num_workers = parsed_args[:num_workers]
  restart = parsed_args[:restart]
  # fork process to complete the task before updating all the data
  child_pid = fork do

    db_conn = DBConnection.new(CONNECTION_OPTS)
    queues = db_conn.query_data(num_workers, restart)
    worker = Worker.new(db_conn)

    threads = [worker]
    (num_workers - 1).times do
      threads << Worker.new(DBConnection.new(CONNECTION_OPTS))
    end

    threads.map! do |thread|
      thread.inqueue_async(queues.pop)
      thread.dequeue_async
    end

    threads_wait = ThreadsWait.new(*threads)
    threads_wait.all_waits
  end

  Process.detach(child_pid)
end

desc 'creates test table and populates it with data'
task :seed do
  ruby 'seed.rb'
end

private

def parse_args(command)
  options = { num_workers: 3, restart: false }
  opt_parser = OptionParser.new do |opts|
    opts.banner = "Usage: rake #{command} [options]"
    opts.on('-wNUM', '--workers=NUM', 'number of workers', Integer) do |num|
      options[:num_workers] = num
    end
    opts.on('-r', '--restart', 'continue task', TrueClass) { options[:restart] = true }
  end

  args = opt_parser.order!(ARGV) {}
  opt_parser.parse!(args)

  puts options
  options
end