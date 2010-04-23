class FiberedRedis  
  require 'em-redis'
  require 'fiber'

  def initialize( args={} )
    if args.any?
      @er = EventMachine::Protocols::Redis.connect( args )
    else
      @er = EventMachine::Protocols::Redis.connect
    end
  end

  # The purpose of this method is to stop duplicating the error callback and 
  # the fiber yield for each fibered redis method call
  def fibered_redis_method_call
    f = Fiber.current
    @er.errback do |code| 
      f.resume("Error code: #{code}") 
    end
    yield(f)
    Fiber.yield
  end

  def method_missing(*argv)
    fibered_redis_method_call do |f| 
      @er.send( argv.shift, *argv ) do |rtn|
        f.resume( rtn )
      end
    end
  end

end

