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

          in_string = false
          in_name = false
          prev_char = nil
          cql.chars.each do |char|
            if char == '"' && prev_char != '\\'
              if in_name
                @tokens << {name: current_token}
                current_token = ''
                in_name = false
              else
                in_name = true
              end
            elsif char == "'" && prev_char != '\\'
              if in_string
                @tokens << {string: current_token}
                current_token = ''
                in_string = false
              else
                in_string = true
              end
            elsif !in_name && !in_string && char == ' '
              @tokens << {(KEYWORD_MAP[current_token.upcase] || :id) => current_token}
              current_token = ''
            elsif char == '\\'
            else
              current_token << char
            end
            prev_char = char
          end
          @tokens << {(KEYWORD_MAP[current_token.upcase] || :id) => current_token} if current_token.present?
        end

      end
    end
  end
end
