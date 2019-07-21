# Connection represent a connection established to a db.
# It also provides query methods to the db.
class DBConnection
  require 'securerandom'
  require 'pg'

  attr_reader :connection

  def initialize(opts = {})
    @connection = PG.connect(opts)
  end

  def query_data(num_workers, restart)
    result = query_data_from_db(num_workers, restart)
    return result.delete!('{}') if num_workers == 1

    (0...num_workers).map do |num|
      result.getvalue(num, 0).delete!('{}')
    end
  end

  def update_uuid(id)
    connection.exec <<-SQL
      UPDATE randoms
      SET random = '#{SecureRandom.uuid}'::uuid
      WHERE id = #{id};
    SQL
  end

  def close
    connection.close
  end

  private

  # restart is meant to be flag
  # which signals to skip  rows with randoms has already been set
  def query_data_from_db(num_workers, restart)
    restart_cond = restart ? 'WHERE random IS NULL' : ''

    connection.exec <<-SQL
    SELECT array_agg(rand.id) FROM (
      SELECT id, id % #{num_workers} AS worker_id
      FROM randoms #{restart_cond}
    ) rand GROUP BY rand.worker_id;
    SQL
  end
end
