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
          tokenize(cql, current_token, in_name, in_string, prev_char)
        end

        private

        def tokenize(cql, current_token, in_name, in_string, prev_char)
          cql.chars.each do |char|
            if char == '"' && prev_char != '\\'
              in_name = tokenize_string(:name, in_name, current_token)
              current_token = '' unless in_name
            elsif char == "'" && prev_char != '\\'
              in_string = tokenize_string(:string, in_string, current_token)
              current_token = '' unless in_string
            elsif !in_name && !in_string && char == ' '
              translate_token(current_token)
              current_token = ''
            elsif char == '\\'
              # do nothing...
            else
                          current_token << char
            end
            prev_char = char
          end
          translate_token(current_token) if current_token.present?
        end

        def tokenize_string(type, in_string, current_token)
          if in_string
            @tokens << {type => current_token}
            false
          else
            true
          end
        end

        def translate_token(current_token)
          @tokens << {(KEYWORD_MAP[current_token.upcase] || :id) => current_token}
        end

      end
    end
  end
end
