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
      f.write("every 2.minutes { puts 'blah' }")
    end
    worker = Delayed::ScheduledWorker.new
    worker.load_schedule
    worker.schedule.items.size.should == 1
    worker.schedule.items.first.first.should == 2.minutes

    File.delete("schedule.rb")
  end

  it "should load a schedule from a specified file" do
    open("custom.rb", "w") do |f|
      f.write("every 2.minutes { puts 'blah' }")
    end
    worker = Delayed::ScheduledWorker.new(:schedule => "custom.rb")
    worker.load_schedule
    worker.schedule.items.size.should == 1
    worker.schedule.items.first.first.should == 2.minutes

    File.delete("custom.rb")
  end
end
