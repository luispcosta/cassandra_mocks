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
          @tokens = []
          current_token = ''
          cql.chars.each do |char|
            if char == ' '
              @tokens << {(KEYWORD_MAP[current_token.upcase] || :id) => current_token}
              current_token = ''
            else
              current_token << char
            end
          end
          @tokens << {(KEYWORD_MAP[current_token.upcase] || :id) => current_token}
        end

      end
    end
  end
end
