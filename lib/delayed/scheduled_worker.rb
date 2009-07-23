module Delayed
  # A worker that processes jobs normally, but also enqueues new jobs based on a cron-like schedule
  class ScheduledWorker < Worker

    attr_accessor :schedule

    def initialize(options={})
      super(options)
      @schedule_file = options[:schedule] || "schedule.rb"
      @schedule = Schedule.new(logger)
    end

    def load_schedule
      if File.exists?(@schedule_file)
        schedule.instance_eval(File.read(@schedule_file))
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

        enqueue_scheduled_tasks

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

    def enqueue_scheduled_tasks
      schedule.jobs.each do |frequency, tasks|
        tasks.each do |task_with_last_queued|
          task, last_queued = task_with_last_queued

          next if last_queued && Time.current < last_queued + frequency

          # Don't queue if an instance of the task is already queued up in the future.
          #
          # This uses the serialized payload object to distinguish between separate tasks.
          # A ScheduledTask object has both the task's command and frequency as its state, so searching
          # on serialized handlers allows this to work. The drawback is that ScheduledTask can't have
          # any state that, when serialized, would make the payload different between the task's Delayed::Job
          # records.
          #
          # This solution does not require extra modification to the delayed_jobs table, but time should 
          # be spent investigating a better solution that is not sensitive to payload state and
          # does not require searching on a TEXT database column. 
          # E.g. a string column that holds the MD5 hash of each distinct task.
          if !Delayed::Job.find(:first, :conditions => ['handler = ? AND run_at > ?', task.to_yaml, Time.now])
            logger.debug "Queuing task #{task.task}" if logger
            Delayed::Job.enqueue(task, 0, Time.now + frequency)
            task_with_last_queued[1] = Time.current
          end
        end
      end
    end

    class Schedule
      attr_accessor :jobs, :logger

      def initialize(logger = nil)
        @jobs = {}
        @logger = logger
      end

      def every(frequency, options={})
        # TODO: Support a more 'whenever'-style interface
        task = options[:run]
        raise ArgumentError, "'every' statement is missing the :run parameter." unless task
        logger.info "Registered scheduled task '#{task}' every #{frequency.inspect}" if logger
        frequency = frequency.to_i
        @jobs[frequency] ||= []
        # second element indicates when the task was last queued. nil means never
        @jobs[frequency] << [ScheduledTask.new(frequency, task), nil]
      end
    end

    class ScheduledTask
      attr_accessor :task, :frequency

      def initialize(frequency, task)
        @frequency = frequency
        @task = task
      end

      def perform
        eval(@task)
      end
    end
  end
end
