module Scenic
  module Adapters
    class Postgres

      # @api private
      class TriggerReapplication
        # Creates the trigger reapplication object.
        #
        # @param connection [Connection] The connection to execute SQL against.
        # @param speaker [#say] (ActiveRecord::Migration) The object used for
        #   logging the results of reapplying triggeres.
        def initialize(connection:, speaker: ActiveRecord::Migration)
          @connection = connection
          @speaker = speaker
        end

        # Caches triggeres on the provided object before executing the block and
        # then reapplying the triggeres. Each recreated or skipped trigger is
        # announced to STDOUT by default. This can be overridden in the
        # constructor.
        #
        # @param name The name of the object we are reapplying triggeres on.
        # @yield Operations to perform before reapplying triggeres.
        #
        # @return [void]
        def on(name)
          triggers = Triggers.new(connection: connection).on(name)

          yield

          triggers.each(&method(:try_trigger_create))
        end

        def try_trigger_create(trigger)
          success = with_savepoint(trigger.name) do
            connection.execute(trigger.definition)
          end

          if success
            say "trigger '#{trigger.name}' on '#{trigger.namespace}.#{trigger.table}' has been recreated"
          else
            say "trigger '#{trigger.name}' on '#{trigger.namespace}.#{trigger.table}' is no longer valid and has been dropped."
          end
        end

        private

        attr_reader :connection, :speaker

        def with_savepoint(name)
          connection.execute("SAVEPOINT #{name}")
          yield
          connection.execute("RELEASE SAVEPOINT #{name}")
          true
        rescue
          connection.execute("ROLLBACK TO SAVEPOINT #{name}")
          false
        end

        def say(message)
          subitem = true
          speaker.say(message, subitem)
        end
      end
    end
  end
end
