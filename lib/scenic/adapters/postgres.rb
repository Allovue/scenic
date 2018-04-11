require_relative "postgres/connection"
require_relative "postgres/errors"
require_relative "postgres/index_reapplication"
require_relative "postgres/trigger_reapplication"
require_relative "postgres/indexes"
require_relative "postgres/triggers"
require_relative "postgres/views"
require_relative "postgres/refresh_dependencies"

module Scenic
  # Scenic database adapters.
  #
  # Scenic ships with a Postgres adapter only but can be extended with
  # additional adapters. The {Adapters::Postgres} adapter provides the
  # interface.
  module Adapters
    # An adapter for managing Postgres views.
    #
    # These methods are used interally by Scenic and are not intended for direct
    # use. Methods that alter database schema are intended to be called via
    # {Statements}, while {#refresh_materialized_view} is called via
    # {Scenic.database}.
    #
    # The methods are documented here for insight into specifics of how Scenic
    # integrates with Postgres and the responsibilities of {Adapters}.
    class Postgres
      # Creates an instance of the Scenic Postgres adapter.
      #
      # This is the default adapter for Scenic. Configuring it via
      # {Scenic.configure} is not required, but the example below shows how one
      # would explicitly set it.
      #
      # @param [#connection] connectable An object that returns the connection
      #   for Scenic to use. Defaults to `ActiveRecord::Base`.
      #
      # @example
      #  Scenic.configure do |config|
      #    config.database = Scenic::Adapters::Postgres.new
      #  end
      def initialize(connectable = ActiveRecord::Base)
        @connectable = connectable
      end

      # Returns an array of views in the database.
      #
      # This collection of views is used by the [Scenic::SchemaDumper] to
      # populate the `schema.rb` file.
      #
      # @return [Array<Scenic::View>]
      def views
        Views.new(connection).all
      end

      # Creates a view in the database.
      #
      # This is typically called in a migration via {Statements#create_view}.
      #
      # @param name The name of the view to create
      # @param sql_definition The SQL schema for the view.
      #
      # @return [void]
      def create_view(name, sql_definition)
        execute "CREATE VIEW #{quote_table_name(name)} AS #{sql_definition};"
      end

      # Updates a view in the database.
      #
      # This results in a {#drop_view} followed by a {#create_view}. The
      # explicitness of that two step process is preferred to `CREATE OR
      # REPLACE VIEW` because the former ensures that the view you are trying to
      # update did, in fact, already exist. Additionally, `CREATE OR REPLACE
      # VIEW` is allowed only to add new columns to the end of an existing
      # view schema. Existing columns cannot be re-ordered, removed, or have
      # their types changed. Drop and create overcomes this limitation as well.
      #
      # This is typically called in a migration via {Statements#update_view}.
      #
      # @param name The name of the view to update
      # @param sql_definition The SQL schema for the updated view.
      # @param cascade Whether to drop and recreate dependent objects or not
      #
      # @return [void]
      def update_view(name, sql_definition, cascade=false)
        if cascade
          # Get existing views that could be dependent on this one.
          # removing leading namespace, if any.
          existing_views = views.reject{|v| v.name.split('.').last == name}

          # Get indexes of existing materialized views
          indexes = Indexes.new(connection: connection)
          view_indexes = existing_views.select(&:materialized).flat_map do |view|
            indexes.on(view.name)
          end
          # Get a list of existing triggers
          triggers = Triggers.new(connection: connection)
          view_triggers = views.map do |view|
            triggers.on(view.name)
          end.flatten
          order_of_views =  order_of_view_dependencies_for(views)
        end
        drop_view(name, cascade)
        create_view(name, sql_definition)

        trigger_reapplier = TriggerReapplication.new(connection: connection)
        lost_triggers = view_triggers.select {|t| t.table == name }
        lost_triggers.each{|trigger| trigger_reapplier.try_trigger_create  trigger}

        if cascade
          recreate_dropped_views(existing_views, views, indexes: view_indexes, triggers: view_triggers, view_order: order_of_views)
        end
      end

      # Replaces a view in the database using `CREATE OR REPLACE VIEW`.
      #
      # This results in a `CREATE OR REPLACE VIEW`. Most of the time the
      # explicitness of the two step process used in {#update_view} is preferred
      # to `CREATE OR REPLACE VIEW` because the former ensures that the view you
      # are trying to update did, in fact, already exist. Additionally,
      # `CREATE OR REPLACE VIEW` is allowed only to add new columns to the end
      # of an existing view schema. Existing columns cannot be re-ordered,
      # removed, or have their types changed. Drop and create overcomes this
      # limitation as well.
      #
      # However, when there is a tangled dependency tree
      # `CREATE OR REPLACE VIEW` can be preferable.
      #
      # This is typically called in a migration via
      # {Statements#replace_view}.
      #
      # @param name The name of the view to update
      # @param sql_definition The SQL schema for the updated view.
      #
      # @return [void]
      def replace_view(name, sql_definition)
        execute "CREATE OR REPLACE VIEW #{quote_table_name(name)} AS #{sql_definition};"
      end

      # Drops the named view from the database
      #
      # This is typically called in a migration via {Statements#drop_view}.
      #
      # @param name The name of the view to drop
      # @param cascade Whether to drop dependent objects or not
      #
      # @return [void]
      def drop_view(name, cascade=false)
        execute "DROP VIEW #{quote_table_name(name)}#{" CASCADE" if cascade};"
      end

      # Creates a materialized view in the database
      #
      # @param name The name of the materialized view to create
      # @param sql_definition The SQL schema that defines the materialized view.
      #
      # This is typically called in a migration via {Statements#create_view}.
      #
      # @raise [MaterializedViewsNotSupportedError] if the version of Postgres
      #   in use does not support materialized views.
      #
      # @return [void]
      def create_materialized_view(name, sql_definition)
        raise_unless_materialized_views_supported
        execute "CREATE MATERIALIZED VIEW #{quote_table_name(name)} AS #{sql_definition};"
      end

      # Updates a materialized view in the database.
      #
      # Drops and recreates the materialized view. Attempts to maintain all
      # previously existing and still applicable indexes on the materialized
      # view after the view is recreated.
      #
      # This is typically called in a migration via {Statements#update_view}.
      #
      # @param name The name of the view to update
      # @param sql_definition The SQL schema for the updated view.
      # @param cascade Whether to drop and recreate dependent objects or not
      #
      # @raise [MaterializedViewsNotSupportedError] if the version of Postgres
      #   in use does not support materialized views.
      #
      # @return [void]
      def update_materialized_view(name, sql_definition, cascade=false)
        raise_unless_materialized_views_supported

        if cascade
          # Get existing views that could be dependent on this one.
          existing_views = views.reject{|v| v.name == name}

          # Get indexes of existing materialized views
          indexes = Indexes.new(connection: connection)
          view_indexes = existing_views.select(&:materialized).flat_map do |view|
            indexes.on(view.name)
          end
          # Get a list of existing triggers
          triggers = Triggers.new(connection: connection)
          view_triggers = existing_views.map do |view|
            triggers.on(view.name)
          end.flatten
          order_of_views = order_of_view_dependencies_for(existing_views)
        end

        IndexReapplication.new(connection: connection).on(name) do
          drop_materialized_view(name, cascade)
          create_materialized_view(name, sql_definition)
        end

        recreate_dropped_views(existing_views, views, indexes: view_indexes, triggers: view_triggers, view_order: order_of_views) if cascade
      end

      # Drops a materialized view in the database
      #
      # This is typically called in a migration via {Statements#update_view}.
      #
      # @param name The name of the materialized view to drop.
      # @param cascade Whether to drop dependent objects or not.
      # @raise [MaterializedViewsNotSupportedError] if the version of Postgres
      #   in use does not support materialized views.
      #
      # @return [void]
      def drop_materialized_view(name, cascade=false)
        raise_unless_materialized_views_supported
        execute "DROP MATERIALIZED VIEW #{quote_table_name(name)}#{" CASCADE" if cascade};"
      end

      # Refreshes a materialized view from its SQL schema.
      #
      # This is typically called from application code via {Scenic.database}.
      #
      # @param name The name of the materialized view to refresh.
      # @param concurrently [Boolean] Whether the refreshs hould happen
      #   concurrently or not. A concurrent refresh allows the view to be
      #   refreshed without locking the view for select but requires that the
      #   table have at least one unique index that covers all rows. Attempts to
      #   refresh concurrently without a unique index will raise a descriptive
      #   error.
      #
      # @raise [MaterializedViewsNotSupportedError] if the version of Postgres
      #   in use does not support materialized views.
      # @raise [ConcurrentRefreshesNotSupportedError] when attempting a
      #   concurrent refresh on version of Postgres that does not support
      #   concurrent materialized view refreshes.
      #
      # @example Non-concurrent refresh
      #   Scenic.database.refresh_materialized_view(:search_results)
      # @example Concurrent refresh
      #   Scenic.database.refresh_materialized_view(:posts, concurrently: true)
      #
      # @return [void]
      def refresh_materialized_view(name, concurrently: false, cascade: false)
        raise_unless_materialized_views_supported
        if cascade
          refresh_dependencies_for(name)
        end

        if concurrently
          raise_unless_concurrent_refresh_supported
          execute "REFRESH MATERIALIZED VIEW CONCURRENTLY #{quote_table_name(name)};"
        else
          execute "REFRESH MATERIALIZED VIEW #{quote_table_name(name)};"
        end
      end

      private

      attr_reader :connectable
      delegate :execute, :quote_table_name, to: :connection

      def connection
        Connection.new(connectable.connection)
      end

      def raise_unless_materialized_views_supported
        unless connection.supports_materialized_views?
          raise MaterializedViewsNotSupportedError
        end
      end

      def raise_unless_concurrent_refresh_supported
        unless connection.supports_concurrent_refreshes?
          raise ConcurrentRefreshesNotSupportedError
        end
      end

      def refresh_dependencies_for(name)
        Scenic::Adapters::Postgres::RefreshDependencies.call(
          name,
          self,
          connection,
        )
      end

      def order_of_view_dependencies_for(array_of_views)
        # de-namespace the views for this check
        query_fragment = array_of_views.
        map(&:name).
        map {|maybe_namespaced| maybe_namespaced.split('.').last }.
        map {|view| "'#{view}'"}.join(', ')
        sql = <<-SQL
          WITH RECURSIVE t AS
            -- Get every view & materialized view, assign a level 0
            ( SELECT c.oid,
                     pg_namespace.nspname,
                     c.relname,
                     0 AS LEVEL
             FROM pg_class c
             JOIN pg_namespace ON c.relnamespace = pg_namespace.oid
             WHERE c.relkind IN ('v', 'm')
             AND c.relname NOT IN (SELECT extname FROM pg_extension)
             -- Only look at views in our current schema search_path
             AND pg_namespace.nspname = ANY (current_schemas(false))
             -- Union back on ourselves, increasing the level to indicate that the view is dependent
             UNION ALL SELECT c.oid,
                              pg_namespace.nspname,
                              c.relname,
                              a.level+1
             FROM t a
             JOIN pg_depend d ON d.refobjid=a.oid
             JOIN pg_rewrite w ON w.oid= d.objid AND w.ev_class!=a.oid
             JOIN pg_class c ON c.oid=w.ev_class
             JOIN pg_namespace ON c.relnamespace = pg_namespace.oid
             AND pg_namespace.nspname = ANY (current_schemas(false))
             )
          -- Take the max level for each view.
          SELECT relname, nspname, MAX(level) AS level
          FROM t
          WHERE relname IN (#{query_fragment})
          GROUP BY relname, nspname
          ORDER BY level asc;
        SQL
        connection.select_rows(sql).map(&:first)
      end

      def recreate_dropped_views(old_views, current_views, indexes: [], triggers: [], view_order: [])
        index_reapplier = IndexReapplication.new(connection: connection)
        trigger_reapplier = TriggerReapplication.new(connection: connection)

        # Find any views that were lost
        dropped_views = old_views.reject{|ov| current_views.any?{|cv| ov.name == cv.name}}
        # Recreate them

        # Merge the list of dropped views with the list of the ordering we got from the original
        # view's dependency tree, removing namespaces so comparisons work
        rebuild_order = view_order.map {|ov| dropped_views.find {|dv| dv.name.split('.').last == ov }}.compact

        rebuild_order.each do |view|
          if view.materialized
            create_materialized_view view.name, view.definition
            # Also recreate any indexes that were lost
            lost_indexes = indexes.select{|index| index.object_name == view.name}
            lost_indexes.each{|index| index_reapplier.try_index_create  index}
            # Also recreate any triggers that were lost

          else
            create_view view.name, view.definition
          end

          lost_triggers = triggers.select {|t| t.table == view.name }
          lost_triggers.each{|trigger| trigger_reapplier.try_trigger_create  trigger}
        end
      end
    end
  end
end
