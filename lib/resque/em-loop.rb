#!/usr/bin/env ruby

module EventMachine

  class FiberPool

    DEFAULT_FIBER_POOL_SIZE = 1000 

    attr_accessor :pause

    def initialize( max_workers = DEFAULT_FIBER_POOL_SIZE )
      @pause = false

      @fibers = []
      max_workers.times do |i|
        @fibers << Fiber.new do |block|
          loop do
            block.call
            @fibers << Fiber.current
            block = Fiber.yield
          end
        end
      end

    end

    #
    # This allows us to pause fiber spawning, useful when halting all fibers from
    # doing work
    #
    def disable_fiber_spawning!
      @pause = false
    end

    def enable_fiber_spawning!
      @pause = true
    end

    def spawn
      unless @pause
        @fibers.shift
      end
    end

  end

  class Loop 

    alias :blocking_sleep :sleep

    attr_accessor :backoff_period

    NO_THROTTLE = -1
    DEFAULT_BACKOFF_PERIOD = 0.1
    DEFAULT_FIBERS = 1000
    DEFAULT_THROTTLE_PERIOD = 1 # seconds

    def initialize
      @throttle = NO_THROTTLE 
      @throttle_period = DEFAULT_THROTTLE_PERIOD
      @fiber_pool = FiberPool.new( DEFAULT_FIBERS )
      @backoff_period = DEFAULT_BACKOFF_PERIOD
    end

    # 
    # If throttle is > 1 then we can assume a time period of 1 sec, otherwise if 
    # it is < 1 we need to increase the time period > 1 sec
    #
    def throttle=( value )
      @throttle = value

      if value.nil?
        @throttle = NO_THROTTLE
      elsif value < 1
        @throttle_period = 1 / value.to_f
      else
        @throttle_period = DEFAULT_THROTTLE_PERIOD
      end

    end

    # 
    # Take 2 arguments t1 = start_time, t2 = end_time. If the difference is less than 1 second
    # then return the difference as a float, this is used for throttling
    #
    def throttle_delay_period( t1, t2 )
      t_delta = t2 - t1
      delay_by = 0
      if t_delta < @throttle_period
        delay_by = @throttle_period - t_delta
      end
      delay_by
    end

    def run( &block )

      itterations = 0
      itter_start_time = Time.now.to_f

      delta = 0.0
      process_next = proc do

        # If we are about to exceed our throttle then do not resume the fiber
        # and delay before calling process_next
        if ( @throttle != NO_THROTTLE and itterations >= @throttle )
          itter_end_time = Time.now.to_f  
          delay = throttle_delay_period( itter_start_time, itter_end_time )

          EM::Timer.new( delay ) do 

            # reset values used for throttling
            itter_start_time = Time.now.to_f
            itterations = 0

            EM.next_tick(process_next) 
          end

          # If we have got here then we are okay to resume the fiber with the 
          # block e.g. process the block
        elsif f = @fiber_pool.spawn
          f.resume( block )
          itterations += 1
          EM.next_tick(process_next)

          # We have no fibers left so backoff to save the cpu from being 
          # hammered
        else
          EM::Timer.new(@backoff_period) do 
            EM.next_tick(process_next) 
          end

        end

      end

      EM.next_tick(process_next)

    end

    # BROKEN!!!!
    #
    # Tell all other workers to finish what they are doing and sleep
    # 
    #def sleep_all_workers( value )
#
#      f = Fiber.current
#      @fiber_pool.disable_fiber_spawning!
#
#      puts "sleeping for #{value}"
#      EM::Timer.new( value ) do
#        puts "resuming"
#        f.resume
#        @fiber_pool.enable_fiber_spawning!
#      end
#
#      Fiber.yield
#    end
#
    #
    # Sleep a fiber one at a time
    #
    def sleep( value )
      f = Fiber.current
      EM::Timer.new( value ) do
        f.resume
      end
      Fiber.yield
    end

  end

end
