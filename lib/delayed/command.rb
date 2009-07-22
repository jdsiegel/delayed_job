require 'rubygems'
require 'daemons'
require 'optparse'

module Delayed
  class Command
    attr_accessor :worker_count

    DEFAULT_SCHEDULE_PATH = "#{RAILS_ROOT}/config/delayed_schedule.rb"
    
    def initialize(args)
      @options = {:quiet => true, :schedule => DEFAULT_SCHEDULE_PATH}
      @worker_count = 1
      
      opts = OptionParser.new do |opts|
        opts.banner = "Usage: #{File.basename($0)} [options] start|stop|restart|run"

        opts.on('-h', '--help', 'Show this message') do
          puts opts
          exit 1
        end
        opts.on('-e', '--environment=NAME', 'Specifies the environment to run this delayed jobs under (test/development/production).') do |e|
          ENV['RAILS_ENV'] = e
        end
        opts.on('--min-priority N', 'Minimum priority of jobs to run.') do |n|
          @options[:min_priority] = n
        end
        opts.on('--max-priority N', 'Maximum priority of jobs to run.') do |n|
          @options[:max_priority] = n
        end
        opts.on('--disable-schedule', 'Turn off cron-like behavior for scheduling repeatable jobs') do
          @options[:disable_schedule] = true
        end
        opts.on('-n', '--number_of_workers=workers', "Number of unique workers to spawn") do |worker_count|
          @worker_count = worker_count.to_i rescue 1
        end
      end
      @args = opts.parse!(args)
    end
  
    def daemonize
      worker_count.times do |worker_index|
        process_name = worker_count == 1 ? "delayed_job" : "delayed_job.#{worker_index}"
        use_scheduled_worker = worker_index == 0 && !@options[:disable_schedule]
        Daemons.run_proc(process_name, :dir => "#{RAILS_ROOT}/tmp/pids", :dir_mode => :normal, :ARGV => @args) do |*args|
          run process_name, use_scheduled_worker
        end
      end
    end
    
    def run(worker_name = nil, use_scheduled_worker=true)
      Dir.chdir(RAILS_ROOT)
      require File.join(RAILS_ROOT, 'config', 'environment')
      
      # Replace the default logger
      logger = Logger.new(File.join(RAILS_ROOT, 'log', 'delayed_job.log'))
      logger.level = ActiveRecord::Base.logger.level
      ActiveRecord::Base.logger = logger
      ActiveRecord::Base.clear_active_connections!
      Delayed::Worker.logger = logger
      Delayed::Job.worker_name = "#{worker_name} #{Delayed::Job.worker_name}"
      
      worker_class = use_scheduled_worker ? Delayed::ScheduledWorker : Delayed::Worker
      worker_class.new(@options).start
    rescue => e
      logger.fatal e
      STDERR.puts e.message
      exit 1
    end
    
  end
end
