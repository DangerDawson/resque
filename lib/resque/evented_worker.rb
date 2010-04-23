module Resque

  class EventedWorker < Worker
    include Resque::Helpers
    extend Resque::Helpers

    # Whether the worker should log basic info to STDOUT
    attr_accessor :verbose

    # Whether the worker should log lots of info to STDOUT
    attr_accessor  :very_verbose

    # How many jobs/sec we wish to process
    attr_accessor  :throttle

    # If a queue does not contain any jobs how long shall we backoff for
    attr_accessor  :backoff_period

    attr_writer :to_s

    # Registers the various signal handlers a worker responds to.
    #
    # TERM: Shutdown immediately, stop processing jobs.
    #  INT: Shutdown immediately, stop processing jobs.
    # QUIT: Shutdown after the current job has finished processing.
    # USR1: Don't process any new jobs
    # CONT: Start processing jobs again after a USR2
    def register_signal_handlers
      trap('TERM') { shutdown }
      trap('INT')  { shutdown }

      begin
        trap('QUIT') { shutdown }
        trap('USR1') { pause_processing }
        trap('CONT') { unpause_processing }
      rescue ArgumentError
        warn "Signals QUIT, USR1, USR2, and/or CONT not supported."
      end

      log! "Registered signals"
    end


    def work(&block)

      $0 = "resque: Starting ( throttle: #{throttle} )"
      Fiber.new { startup }.resume

      # Set up the evented_loop
      evented_loop = EventMachine::Loop.new()
      evented_loop.throttle = @throttle
      evented_loop.backoff_period = @backoff_period
      evented_loop.run do

        log! "just went past #{@shutdown}"
        EM.stop if @shutdown

        if not @paused and job = reserve
          log "got: #{job.inspect}"
          procline "Processing #{job.queue} since #{Time.now.to_i}"

          working_on job
          perform(job, &block)
          done_working

        else
          # If there was nothing to do then let the loop pause for a second or two
          log! "Sleeping for #{backoff_period}"
          evented_loop.sleep(backoff_period.to_i)
        end

      end # l.loop

    end

  end

end
