module Cassandra
  module Mocks
    class ResultPage < Array
      attr_accessor :execution_info

      def initialize(*args)
        @execution_info = {}
        super(*args)
      end

      def last_page?
        true
      end

      def next_page
      end

      def next_page_async
        Cassandra::Future.value(next_page)
      end

      def paging_state
      end

    end
  end
end
