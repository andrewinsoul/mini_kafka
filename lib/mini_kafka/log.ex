defmodule MiniKafka.Log do
  use GenServer

  # path to the event log file for persistence
  @log_file "data/events.log"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def append(event) do
    GenServer.call(__MODULE__, {:append, event})
  end

  @impl true
  def init(_opts) do
    File.mkdir_p!("data")

    {:ok, file} = File.open(@log_file, [:append, :binary])

    offset = recover_offset()

    {:ok, %{file: file, offset: offset}}
  end

  defp recover_offset do
    case File.read(@log_file) do
      {:ok, ""} ->
        0

      {:ok, contents} ->
        offset =
          contents
          |> String.split("\n", trim: true)
          |> List.last()
          |> String.split("|", parts: 2)
          |> hd()
          |> String.to_integer()

        offset + 1

      {:error, :enoent} ->
        0
    end
  end

  def read(offset) do
    GenServer.call(__MODULE__, {:read, offset})
  end

  @impl true
  def handle_call({:read, offset}, _from, state) do
    events =
      @log_file
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.map(fn record ->
        [event_offset, event] = String.split(record, "|", parts: 2)

        %{
          offset: String.to_integer(event_offset),
          event: event
        }
      end)
      |> Enum.filter(fn event ->
        event.offset >= offset
      end)

    {:reply, {:ok, events}, state}
  end

  @impl true
  def handle_call({:append, event}, _from, state) do
    offset = state.offset

    record = "#{offset}|#{event}\n"

    IO.binwrite(state.file, record)

    {:reply, {:ok, offset}, %{state | offset: offset + 1}}
  end
end
