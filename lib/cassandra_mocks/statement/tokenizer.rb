module Cassandra
  module Mocks
    class Statement
      class Tokenizer
        #noinspection RubyStringKeysInHashInspection
        KEYWORD_MAP = {
            'INSERT' => :insert,
            'VALUES' => :values,
            'SELECT' => :select,
            'DELETE' => :delete,
            'FROM' => :from,
            'WHERE' => :where,
            'AND' => :and,
            'IN' => :in,
            'NOT' => :not,
            '(' => :lparen,
            ')' => :rparen,
            '<' => :ltri,
            '>' => :rtri,
            ',' => :comma,
            '.' => :dot,
            '[' => :lbracket,
            ']' => :rbracket,
            '=' => :eql,
            '*' => :star,
            '?' => :parameter,
        }

        attr_reader :tokens

        def initialize(cql)
          @tokens = cql.split.map { |keyword| {(KEYWORD_MAP[keyword.upcase] || :id) => keyword} }
        end

      end
    end
  end
end
