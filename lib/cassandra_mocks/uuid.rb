module Cassandra
  class Uuid
    def <=>(rhs)
      to_s <=> rhs.to_s
    end
  end
end
