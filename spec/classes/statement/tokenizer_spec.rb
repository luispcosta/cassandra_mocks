require 'rspec'

module Cassandra
  module Mocks
    class Statement
      describe Statement do

        let(:statement) { '' }

        subject { Tokenizer.new(statement) }

        describe '#tokens' do
          describe 'special keywords' do

            shared_examples_for 'a token from a reserved keyword' do |keyword, token|
              let(:statement) { keyword }

              it "should parse '#{keyword}' as a(n) '#{token}' token" do
                expect(subject.tokens).to eq(tokenize_expected [token => keyword])
              end

              context 'with a different case' do
                let(:statement) { keyword.downcase }

                it "should parse '#{keyword.downcase}' as a(n) '#{token}' token" do
                  expect(subject.tokens).to eq(tokenize_expected [token => keyword.downcase])
                end

              end
            end

            it_behaves_like 'a token from a reserved keyword', 'CREATE', :create
            it_behaves_like 'a token from a reserved keyword', 'DROP', :drop
            it_behaves_like 'a token from a reserved keyword', 'TRUNCATE', :truncate
            it_behaves_like 'a token from a reserved keyword', 'PRIMARY', :primary
            it_behaves_like 'a token from a reserved keyword', 'KEY', :key
            it_behaves_like 'a token from a reserved keyword', 'TABLE', :table
            it_behaves_like 'a token from a reserved keyword', 'KEYSPACE', :keyspace
            it_behaves_like 'a token from a reserved keyword', 'INSERT', :insert
            it_behaves_like 'a token from a reserved keyword', 'UPDATE', :update
            it_behaves_like 'a token from a reserved keyword', 'SET', :set
            it_behaves_like 'a token from a reserved keyword', 'VALUES', :values
            it_behaves_like 'a token from a reserved keyword', 'SELECT', :select
            it_behaves_like 'a token from a reserved keyword', 'DELETE', :delete
            it_behaves_like 'a token from a reserved keyword', 'FROM', :from
            it_behaves_like 'a token from a reserved keyword', 'WHERE', :where
            it_behaves_like 'a token from a reserved keyword', 'LIMIT', :limit
            it_behaves_like 'a token from a reserved keyword', 'ORDER', :order
            it_behaves_like 'a token from a reserved keyword', 'BY', :by
            it_behaves_like 'a token from a reserved keyword', 'ASC', :asc
            it_behaves_like 'a token from a reserved keyword', 'DESC', :desc
            it_behaves_like 'a token from a reserved keyword', 'AND', :and
            it_behaves_like 'a token from a reserved keyword', 'IN', :in
            it_behaves_like 'a token from a reserved keyword', 'IF', :if
            it_behaves_like 'a token from a reserved keyword', 'NOT', :not
            it_behaves_like 'a token from a reserved keyword', 'EXISTS', :exists
            it_behaves_like 'a token from a reserved keyword', '(', :lparen
            it_behaves_like 'a token from a reserved keyword', ')', :rparen
            it_behaves_like 'a token from a reserved keyword', '<', :ltri
            it_behaves_like 'a token from a reserved keyword', '>', :rtri
            it_behaves_like 'a token from a reserved keyword', ',', :comma
            it_behaves_like 'a token from a reserved keyword', '.', :dot
            it_behaves_like 'a token from a reserved keyword', '[', :lbracket
            it_behaves_like 'a token from a reserved keyword', ']', :rbracket
            it_behaves_like 'a token from a reserved keyword', '=', :eql
            it_behaves_like 'a token from a reserved keyword', '+', :plus
            it_behaves_like 'a token from a reserved keyword', '-', :minus
            it_behaves_like 'a token from a reserved keyword', '*', :star
            it_behaves_like 'a token from a reserved keyword', '?', :parameter

            context 'unrecognized keywords' do
              it_behaves_like 'a token from a reserved keyword', 'Bob', :id
              it_behaves_like 'a token from a reserved keyword', 'Karen', :id
            end

          end

          describe 'numerics' do
            describe 'integers' do
              let(:statement) { rand(0..100).to_s }

              it 'should be able to parse integral numerics' do
                expect(subject.tokens).to eq(tokenize_expected [int: statement])
              end
            end

            describe 'floats' do
              let(:statement) { (rand * 100.0).to_s }

              it 'should be able to parse integral numerics' do
                expect(subject.tokens).to eq(tokenize_expected [float: statement])
              end
            end
          end

          describe 'string parsing' do
            let(:statement) { "'hello, world'" }

            it 'should be able to build a string' do
              expect(subject.tokens).to eq(tokenize_expected [string: 'hello, world'])
            end

            context 'with escaped quotes' do
              let(:statement) { "'\\'hello world\\''" }

              it 'should be able to build a string' do
                expect(subject.tokens).to eq(tokenize_expected [string: "'hello world'"])
              end
            end
          end

          describe 'name parsing' do
            let(:statement) { '"hello, world"' }

            it 'should be able to build a name' do
              expect(subject.tokens).to eq(tokenize_expected [name: 'hello, world'])
            end

            context 'with escaped quotes' do
              let(:statement) { '"\\"hello world\""' }

              it 'should be able to build a name' do
                expect(subject.tokens).to eq(tokenize_expected [name: '"hello world"'])
              end
            end
          end

          describe 'tokenizing well structured statements' do
            let(:statement) { 'SELECT * FROM everything WHERE something = ? AND nothing IN ( 1 , 2 )' }

            it 'should split the statement into tokens' do
              expected_tokens = [
                  {select: 'SELECT'},
                  {star: '*'},
                  {from: 'FROM'},
                  {id: 'everything'},
                  {where: 'WHERE'},
                  {id: 'something'},
                  {eql: '='},
                  {parameter: '?'},
                  {and: 'AND'},
                  {id: 'nothing'},
                  {in: 'IN'},
                  {lparen: '('},
                  {int: '1'},
                  {comma: ','},
                  {int: '2'},
                  {rparen: ')'},
              ]
              expect(subject.tokens).to eq(tokenize_expected expected_tokens)
            end
          end

          describe 'tokenizing statements lacking spaces' do
            let(:statement) { 'SELECT * FROM everything WHERE something<=? AND something_else>=? AND nothing IN (1+3,2-1)' }

            it 'should split the statement into tokens' do
              expected_tokens = [
                  {select: 'SELECT'},
                  {star: '*'},
                  {from: 'FROM'},
                  {id: 'everything'},
                  {where: 'WHERE'},
                  {id: 'something'},
                  {ltri: '<'},
                  {eql: '='},
                  {parameter: '?'},
                  {and: 'AND'},
                  {id: 'something_else'},
                  {rtri: '>'},
                  {eql: '='},
                  {parameter: '?'},
                  {and: 'AND'},
                  {id: 'nothing'},
                  {in: 'IN'},
                  {lparen: '('},
                  {int: '1'},
                  {plus: '+'},
                  {int: '3'},
                  {comma: ','},
                  {int: '2'},
                  {minus: '-'},
                  {int: '1'},
                  {rparen: ')'},
              ]
              expect(subject.tokens).to eq(tokenize_expected expected_tokens)
            end
          end

          describe 'tokenizing namespaced items' do
            let(:statement) { 'SELECT * FROM keyspace_name.table_name' }

            it 'should split the statement into tokens' do
              expected_tokens = [
                  {select: 'SELECT'},
                  {star: '*'},
                  {from: 'FROM'},
                  {id: 'keyspace_name'},
                  {dot: '.'},
                  {id: 'table_name'},
              ]
              expect(subject.tokens).to eq(tokenize_expected expected_tokens)
            end
          end

        end

        describe '#token_queue' do
          let(:statement) { 'SELECT * FROM everything' }

          it 'should build a queue from the list of tokens' do
            results = []
            queue = subject.token_queue
            results << queue.pop until queue.empty?
            expect(results).to eq(subject.tokens)
          end
        end

        private

        def tokenize_expected(tokens)
          tokens.map { |token_attributes| Token.new(*token_attributes.first) }
        end

      end
    end
  end
end
