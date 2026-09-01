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

    {:ok, %{file: file, offset: 0}}
  end

  @impl true
  def handle_call({:append, event}, _from, state) do
    offset = state.offset

    record = "#{offset}|#{event}\n"

    IO.binwrite(state.file, record)

    {:reply, {:ok, offset}, %{state | offset: offset + 1}}
  end
end
