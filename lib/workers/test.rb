module Test
  @queue = :test
  def self.perform( params )
    puts params 
  end
end
