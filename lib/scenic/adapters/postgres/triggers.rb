module Scenic
  module Adapters
    class Postgres
      # Fetches triggers on objects from the Postgres connection.
      #
      # @api private
      class Triggers
        def initialize(connection:)
          @connection = connection
        end

        # Triggers on the provided object.
        #
        # @param name [String] The name of the object we want triggers from.
        # @return [Array<Scenic::Trigger>]
        def on(name)
          triggers_on(name).map(&method(:trigger_from_database))
        end

        private

        attr_reader :connection
        delegate :quote_table_name, to: :connection

        def triggers_on(name)
          view_name = name.split('.').last
          connection.execute(<<-SQL)
      SELECT array_to_string(array_agg(event_manipulation::varchar), ' OR ') as event_manipulation,
      event_object_schema, event_object_table, trigger_name, action_statement,
      action_orientation, action_timing
      FROM information_schema.triggers
      WHERE event_object_table = '#{view_name}'
      AND event_object_schema = ANY (current_schemas(false))
      GROUP BY event_object_table,
      trigger_name, action_statement,
      action_orientation, action_timing, event_object_schema
          SQL
        end

        def trigger_from_database(result)
          Scenic::Trigger.new(
            event: result["event_manipulation"],
            namespace: result["event_object_schema"],
            table: result["event_object_table"],
            name: result["trigger_name"],
            action: result['action_statement'],
            scope: result['action_orientation'],
            timing: result['action_timing']
          )
        end
      end
    end
  end
end
