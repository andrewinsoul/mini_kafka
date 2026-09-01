defmodule MiniKafkaTest do
  use ExUnit.Case
  doctest MiniKafka

  test "greets the world" do
    assert MiniKafka.hello() == :world
  end
end
