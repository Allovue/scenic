module Scenic
  # The in-memory representation of a database index.
  #
  # **This object is used internally by adapters and the schema dumper and is
  # not intended to be used by application code. It is documented here for
  # use by adapter gems.**
  #
  # @api extension
  class Trigger

  	attr_reader :event, :table, :name, :action, :scope, :timing

  	def definition
  	<<-DDL
  	  CREATE TRIGGER #{name}
  	  #{@timing} #{@event}
  	  ON #{@table}
  	  FOR EACH #{@scope}
  	  #{@action};
  	DDL
  	end

    def initialize(event:, table:, name:, action:, scope:, timing:)
      @event = event
      @table = table
      @name  = name
      @action = action
      @scope = scope
      @timing = timing
    end

    def ==(index)
      @event == index.event &&
        @table = index.table &&
        @name == index.name &&
        @action = index.action &&
        @scope = index.scope &&
        @timing = index.timing
    end
  end
end
