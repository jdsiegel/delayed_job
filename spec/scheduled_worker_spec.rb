require File.dirname(__FILE__) + '/database'

describe Delayed::ScheduledWorker do
  it "should ignore a missing schedule file" do
    worker = Delayed::ScheduledWorker.new(:schedule => "missing")
    lambda {
      worker.load_schedule
    }.should_not raise_error
  end

  it "should load a schedule from the default file" do
    open("schedule.rb", "w") do |f|
      f.write("every 2.minutes, :run => '3+4'")
    end
    worker = Delayed::ScheduledWorker.new
    worker.load_schedule
    worker.schedule.jobs.keys.should include(2.minutes)

    File.delete("schedule.rb")
  end

  it "should load a schedule from a specified file" do
    open("custom.rb", "w") do |f|
      f.write("every 2.minutes, :run => '3+4'")
    end
    worker = Delayed::ScheduledWorker.new(:schedule => "custom.rb")
    worker.load_schedule
    worker.schedule.jobs.keys.should include(2.minutes)

    File.delete("custom.rb")
  end

  it "should queue a task that has no job in the future" do
    worker = Delayed::ScheduledWorker.new
    worker.schedule.every 2.minutes, :run => "Klass.some_method"
    worker.enqueue_scheduled_tasks
    Delayed::Job.count.should == 1 
    Delayed::Job.first.run_at.to_s.should == (Time.current + 2.minutes).to_s

    task, last_queued = worker.schedule.jobs[2.minutes].first
    Delayed::Job.first.handler.should == task.to_yaml
    last_queued.to_s.should == Time.current.to_s
  end

  it "should not queue a task before the previous task instance is executed" do
    worker = Delayed::ScheduledWorker.new
    worker.schedule.every 2.minutes, :run => "Klass.some_method"

    task_with_last_queued = worker.schedule.jobs[2.minutes].first
    task_with_last_queued[1] = Time.current - 30.seconds

    lambda {
      worker.enqueue_scheduled_tasks
    }.should_not change(Delayed::Job, :count)
  end

  it "should not queue a task with a pending job in the future" do
    worker = Delayed::ScheduledWorker.new
    worker.schedule.every 2.minutes, :run => "Klass.some_method"

    task, last_queued = worker.schedule.jobs[2.minutes].first
    Delayed::Job.enqueue(task, 0, Time.now + 2.minutes)

    lambda {
      worker.enqueue_scheduled_tasks
    }.should_not change(Delayed::Job, :count)
  end
end

describe Delayed::ScheduledWorker::ScheduledTask do
  it "should be unique on frequency and command when serialized" do
    t1 = Delayed::ScheduledWorker::ScheduledTask.new(5.minutes, "some_command")
    t2 = Delayed::ScheduledWorker::ScheduledTask.new(5.minutes, "some_command")
    t3 = Delayed::ScheduledWorker::ScheduledTask.new(10.minutes, "some_command")
    t4 = Delayed::ScheduledWorker::ScheduledTask.new(5.minutes, "different_command")

    t1.to_yaml.should == t2.to_yaml
    t1.to_yaml.should_not == t3.to_yaml
    t1.to_yaml.should_not == t4.to_yaml
  end
end
