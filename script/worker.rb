#!/usr/bin/env ruby

$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'resque'
require 'optparse'
require 'pp'
require 'workers/test'

usage = "Usage: worker.rb [options] queues"

options = {}
optparse = OptionParser.new do|opts|

  # Set a banner, displayed at the top
  opts.banner = usage 

  # Define the options, and what they do
  
  options[:verbose] = false
  opts.on( '-v', '--verbose', 'Output logging to stdout' ) do
    options[:verbose] = true
  end

  options[:vverbose] = false
  opts.on( '-vv', '--vverbose', 'Output even more logging to stdout' ) do
    options[:vverbose] = true
  end

  options[:throttle] = 100 
  opts.on('-t', '--throttle NUMBER', Integer, "Process messages at (default #{options[:throttle]})") do |throttle|
    options[:throttle] = throttle
  end 

  options[:backoff_period] = 5
  opts.on( '-b', '--backoff_period SECONDS', Integer, "Backoff if there is nothing to process ( default #{options[:backoff_period]} )" ) do |backoff_period|
    options[:backoff_period] = backoff_period
  end

  # This displays the help screen
  opts.on( '-h', '--help', 'Display this screen' ) do
    puts opts
    exit
  end

end

# Parse the command-line. 
optparse.parse!
puts options

queues = ARGV

worker = nil
begin
  worker = Resque::EventedWorker.new(*queues)

  worker.verbose = options[:verbose] 
  worker.very_verbose = options[:vverbose]

  worker.throttle = options[:throttle].to_i
  worker.backoff_period = options[:backoff_period].to_i

rescue Resque::NoQueueError
  abort usage 
end

puts "*** Starting worker #{worker}"

# Not sure this should be here
EM.run do
  worker.work
end
