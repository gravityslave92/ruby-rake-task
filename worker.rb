# worker is a wrapper for thread synchronizing
# which performs update via db connection
class Worker
  attr_reader :connection, :queue, :mutex, :cond

  def initialize(connection)
    @connection = connection
    @queue = Queue.new
    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  def inqueue_async(ids_string)
    # ids_string contains ids from db
    # which  are aggregated to a string and delimited by ','
    Thread.new do
      ids_string.each_line(',') do |id|
        queue << id.to_i
        cond.signal
      end

      queue.close
    end
  end

  def dequeue_async
    Thread.new do
      loop do
        break(connection.close) if queue_finished?

        update_uuid
      end
    end
  end

  private

  def queue_finished?
    queue.closed? && queue.empty?
  end

  def update_uuid
    mutex.synchronize do
      cond.wait(mutex) while queue.empty?
      connection.update_uuid(queue.pop)
    end
  rescue PG::Error
    # kill the main process to stop all threads
    Process.exit(1)
  end
end
