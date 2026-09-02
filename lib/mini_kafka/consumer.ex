defmodule MiniKafka.Consumer do
  use GenServer

  @offset_file "data/consumer.offset"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    offset =
      case File.read(@offset_file) do
        {:ok, value} ->
          String.to_integer(value)

        {:error, :enoent} ->
          0
      end

    {:ok, %{offset: offset}}
  end

  def consume do
    GenServer.cast(__MODULE__, :consume)
  end

  defp handle_event(%{event: "user_signed_up"} = event) do
    IO.inspect(event, label: "Consumed event for created user")
  end

  defp handle_event(%{event: "user_logged_in"} = event) do
    IO.inspect(event, label: "Consumed event for updated user")
  end

  defp handle_event(%{event: "user_subscribed_nature_podcast"} = event) do
    IO.inspect(event, label: "Consumed event for user subscribed to Nature podcast")
  end

  @impl true
  def handle_cast(:consume, state) do
    {:ok, events} = MiniKafka.Log.read(state.offset)

    Enum.each(events, fn event ->
      handle_event(event)
    end)

    new_offset =
      case List.last(events) do
        nil -> state.offset
        event -> event.offset + 1
      end

    File.write!(@offset_file, Integer.to_string(new_offset))

    {:noreply, %{state | offset: new_offset}}
  end
end
