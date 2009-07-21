module Delayed
  # A worker that processes jobs normally, but also enqueues new jobs based on a cron-like schedule
  class ScheduledWorker < Worker
    class Schedule
      attr_accessor :items

      def initialize
        @items = []
      end

      def every(duration, &proc)
        @items << [duration, proc]
      end
    end

    attr_accessor :schedule

    def initialize(options={})
      super(options)
      @schedule_file = options[:schedule] || "schedule.rb"
      @schedule = Schedule.new
    end

    def load_schedule
      if File.exists?(@schedule_file)
        schedule.instance_eval(open(@schedule_file) {|f| f.read})
      end
    end

    def start
      # This is copied straight from Delayed::Worker. Could use some refactoring
      say "*** Starting scheduled job worker #{Delayed::Job.worker_name}"

      trap('TERM') { say 'Exiting...'; $exit = true }
      trap('INT')  { say 'Exiting...'; $exit = true }

      load_schedule

      loop do
        result = nil

        realtime = Benchmark.realtime do
          result = Delayed::Job.work_off
        end

        # TODO: Queue scheduled jobs here

        count = result.sum

        break if $exit

        if count.zero?
          sleep(SLEEP)
        else
          say "#{count} jobs processed at %.4f j/s, %d failed ..." % [count / realtime, result.last]
        end

        break if $exit
      end

    ensure
      Delayed::Job.clear_locks!
    end
  end
end
