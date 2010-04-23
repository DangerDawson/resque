#!/usr/bin/env ruby

$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'resque'
require 'workers/test'

# TODO: sort this out
EM.run do
  Fiber.new do

    2.times do
      Resque.enqueue(Test, Time.now.to_s )
    end
    EM.stop

  end .resume
end
