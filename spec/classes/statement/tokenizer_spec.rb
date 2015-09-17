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
                expect(subject.tokens).to eq([token => keyword])
              end

              context 'with a different case' do
                let(:statement) { keyword.downcase }

                it "should parse '#{keyword.downcase}' as a(n) '#{token}' token" do
                  expect(subject.tokens).to eq([token => keyword.downcase])
                end

              end
            end

            it_behaves_like 'a token from a reserved keyword', 'INSERT', :insert
            it_behaves_like 'a token from a reserved keyword', 'VALUES', :values
            it_behaves_like 'a token from a reserved keyword', 'SELECT', :select
            it_behaves_like 'a token from a reserved keyword', 'DELETE', :delete
            it_behaves_like 'a token from a reserved keyword', 'FROM', :from
            it_behaves_like 'a token from a reserved keyword', 'WHERE', :where
            it_behaves_like 'a token from a reserved keyword', 'AND', :and
            it_behaves_like 'a token from a reserved keyword', 'IN', :in
            it_behaves_like 'a token from a reserved keyword', 'NOT', :not

          end


        end

      end
    end
  end
end
