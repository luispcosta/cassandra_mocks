module Cassandra
  module Mocks
    class Statement
      class Token < Struct.new(:type, :value)
      end
    end
  end
end
