defmodule FLAME.PoolGrowTest.FailingBackend do
  # The local backend, except that a boot fails while its counter is above zero,
  # taking one off per failure. The counter lives in `:persistent_term` under the
  # `:fail_key` backend option.
  @behaviour FLAME.Backend

  alias FLAME.LocalBackend

  def fail_next(key, n), do: :counters.put(counter(key), 1, n)

  defp counter(key) do
    case :persistent_term.get({__MODULE__, key}, nil) do
      nil ->
        ref = :counters.new(1, [:atomics])
        :persistent_term.put({__MODULE__, key}, ref)
        ref

      ref ->
        ref
    end
  end

  @impl true
  def init(opts), do: LocalBackend.init(opts)

  @impl true
  def remote_spawn_monitor(state, term), do: LocalBackend.remote_spawn_monitor(state, term)

  @impl true
  def system_shutdown, do: LocalBackend.system_shutdown()

  @impl true
  def remote_boot(state) do
    ref = counter(state.fail_key)

    if :counters.get(ref, 1) > 0 do
      :counters.sub(ref, 1, 1)
      {:error, :unavailable}
    else
      LocalBackend.remote_boot(state)
    end
  end
end

defmodule FLAME.PoolGrowTest do
  use ExUnit.Case, async: true

  alias FLAME.Pool
  alias FLAME.PoolGrowTest.FailingBackend

  setup config do
    runner_sup = Module.concat(config.test, "RunnerSup")
    opts = Keyword.merge(config.runner, name: config.test)

    opts =
      if opts[:backend] == :failing,
        do: Keyword.put(opts, :backend, {FailingBackend, fail_key: config.test}),
        else: opts

    start_supervised!({Pool, opts})
    # the pool worker, not its supervisor, so a worker restart is caught
    {:ok, runner_sup: runner_sup, pool_pid: Process.whereis(config.test)}
  end

  def slow_grow_start(_meta), do: Process.sleep(300)

  # Sends the test process every growth callback, with its reason.
  def report_grow_start(meta) do
    if pid = :persistent_term.get({__MODULE__, :listener, meta.name}, nil),
      do: send(pid, {:grow_start, meta.reason})
  end

  defp listen(config) do
    :persistent_term.put({__MODULE__, :listener, config.test}, self())
    on_exit(fn -> :persistent_term.erase({__MODULE__, :listener, config.test}) end)
  end

  defp runners(runner_sup) do
    for {_, pid, :worker, [FLAME.Runner]} <- Supervisor.which_children(runner_sup), do: pid
  end

  defp wait_for(fun, tries \\ 100) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition not met in time")

      true ->
        Process.sleep(20)
        wait_for(fun, tries - 1)
    end
  end

  # Raises in the boot task for pools whose name is marked as failing.
  def failing_grow_start(meta) do
    if :persistent_term.get({__MODULE__, :fail, meta.name}, false), do: raise("boom")
  end

  describe "a check-in that races its runner going down" do
    # Seen on staging 2026-11-24: a runner was stopped while callers were still
    # using it. The pool handled the runner's :DOWN first (drop_child_runner
    # removes and kills its callers), then read a {:cancel, ref, pid, :catch}
    # one of those callers had already sent, and raised "expected to checkin
    # runner ... that does not exist", taking the whole pool down.
    @tag runner: [min: 2, max: 2, max_concurrency: 2]
    test "is ignored instead of crashing the pool", %{pool_pid: pool_pid} = config do
      test_pid = self()

      caller =
        spawn(fn ->
          FLAME.call(config.test, fn ->
            send(test_pid, :in_call)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :in_call, 2000

      %{callers: %{^caller => %{checkout_ref: ref, runner_ref: runner_ref}}, runners: runners} =
        :sys.get_state(pool_pid)

      %{pid: runner_pid} = Map.fetch!(runners, runner_ref)

      # The runner's :DOWN lands in the mailbox ahead of the caller's cancel.
      :sys.suspend(pool_pid)
      Process.exit(runner_pid, :kill)
      wait_for(fn -> not Process.alive?(runner_pid) end)
      send(pool_pid, {:cancel, ref, caller, :catch})
      :sys.resume(pool_pid)

      # Still the same pool process, and still serving calls.
      assert :sys.get_state(pool_pid).callers == %{}
      assert Process.whereis(config.test) == pool_pid
      assert FLAME.call(config.test, fn -> :works end) == :works
    end
  end

  describe "grow/2" do
    @tag runner: [min: 1, max: 3, max_concurrency: 2]
    test "boots one more runner and replies with it", %{runner_sup: runner_sup} = config do
      [first] = runners(runner_sup)
      assert {:ok, pid} = Pool.grow(config.test)
      assert is_pid(pid)
      assert pid != first
      assert Enum.sort(runners(runner_sup)) == Enum.sort([first, pid])
      assert FLAME.call(config.test, fn -> :works end) == :works
    end

    @tag runner: [min: 1, max: 2, max_concurrency: 2, idle_shutdown_after: 100]
    test "a grown runner is not shut down as idle", %{runner_sup: runner_sup} = config do
      assert {:ok, pid} = Pool.grow(config.test)
      ref = Process.monitor(pid)
      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 500
      assert length(runners(runner_sup)) == 2
    end

    @tag runner: [
           min: 1,
           max: 2,
           max_concurrency: 2,
           on_grow_start: &__MODULE__.slow_grow_start/1
         ]
    test "a caller that goes away before the reply leaves the pool working",
         %{runner_sup: runner_sup} = config do
      caller = spawn(fn -> Pool.grow(config.test) end)
      ref = Process.monitor(caller)
      # the boot is held for 300 ms, so the caller dies while it is pending
      Process.sleep(100)
      Process.exit(caller, :kill)
      assert_receive {:DOWN, ^ref, :process, ^caller, :killed}

      wait_for(fn -> length(runners(runner_sup)) == 2 end)
      assert Process.whereis(config.test) == config.pool_pid
      assert FLAME.call(config.test, fn -> :works end) == :works
    end

    @tag runner: [min: 1, max: 1, max_concurrency: 2]
    test "refuses at max without booting", %{runner_sup: runner_sup} = config do
      assert Pool.grow(config.test) == {:error, :max_runners}
      assert length(runners(runner_sup)) == 1
    end

    @tag runner: [
           min: 0,
           max: 2,
           max_concurrency: 2,
           on_grow_start: &__MODULE__.failing_grow_start/1
         ]
    test "replies with an error when the boot fails", %{runner_sup: runner_sup} = config do
      :persistent_term.put({__MODULE__, :fail, config.test}, true)
      on_exit(fn -> :persistent_term.erase({__MODULE__, :fail, config.test}) end)

      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, _reason} = Pool.grow(config.test)
      end)

      assert runners(runner_sup) == []
      assert Process.whereis(config.test) == config.pool_pid
    end

    @tag runner: [min: 0, max: 2, max_concurrency: 2, backend: :failing]
    test "replies with an error when the backend refuses the boot",
         %{runner_sup: runner_sup} = config do
      FailingBackend.fail_next(config.test, 1)

      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, _reason} = Pool.grow(config.test)
      end)

      assert runners(runner_sup) == []
      assert Process.whereis(config.test) == config.pool_pid
      assert {:ok, _pid} = Pool.grow(config.test)
    end

    @tag runner: [min: 0, max: 1, max_concurrency: 1, backend: :failing]
    test "a demand retry does not boot past max after grow took the slot",
         %{runner_sup: runner_sup} = config do
      # the first demand boot fails, which schedules a retry a second later
      FailingBackend.fail_next(config.test, 1)
      parent = self()

      ExUnit.CaptureLog.capture_log(fn ->
        spawn_link(fn ->
          FLAME.call(config.test, fn -> send(parent, :served) end, timeout: 5_000)
        end)

        Process.sleep(200)
        # grow takes the only slot before the retry fires
        assert {:ok, _pid} = Pool.grow(config.test)
        assert_receive :served, 2_000
        Process.sleep(1_200)
      end)

      assert length(runners(runner_sup)) == 1
    end
  end

  describe "restoring min" do
    @tag runner: [min: 1, max: 2, max_concurrency: 2]
    test "is off unless the pool asks for it", %{runner_sup: runner_sup} do
      [runner] = runners(runner_sup)
      Process.exit(runner, :kill)
      wait_for(fn -> runners(runner_sup) == [] end)
      Process.sleep(1_300)
      assert runners(runner_sup) == []
    end

    @tag runner: [restore_min: true, min: 3, max: 4, max_concurrency: 2]
    test "replaces several runners that go down together", %{runner_sup: runner_sup} do
      [a, b, c] = runners(runner_sup)
      Process.exit(a, :kill)
      Process.exit(b, :kill)

      wait_for(fn ->
        current = runners(runner_sup)
        length(current) == 3 and a not in current and b not in current
      end)

      assert c in runners(runner_sup)
      Process.sleep(1_300)
      assert length(runners(runner_sup)) == 3
    end

    @tag runner: [restore_min: true, min: 1, max: 2, max_concurrency: 2, single_use: true]
    test "single-use pools are never restored", %{runner_sup: runner_sup} do
      [runner] = runners(runner_sup)
      Process.exit(runner, :kill)
      wait_for(fn -> runners(runner_sup) == [] end)
      Process.sleep(1_300)
      assert runners(runner_sup) == []
    end

    @tag runner: [restore_min: true, min: 2, max: 3, max_concurrency: 2]
    test "a running runner that goes down is replaced up to min",
         %{runner_sup: runner_sup} = config do
      [victim, survivor] = runners(runner_sup)
      Process.exit(victim, :kill)

      wait_for(fn ->
        current = runners(runner_sup)
        length(current) == 2 and victim not in current
      end)

      assert survivor in runners(runner_sup)
      assert FLAME.call(config.test, fn -> :works end) == :works
    end

    @tag runner: [restore_min: true, min: 1, max: 3, max_concurrency: 2]
    test "no replacement while the pool is still at or above min",
         %{runner_sup: runner_sup} = config do
      [first] = runners(runner_sup)
      assert {:ok, grown} = Pool.grow(config.test)
      Process.exit(first, :kill)
      wait_for(fn -> first not in runners(runner_sup) end)

      # the debounce is 1 s; nothing is booted after it either
      Process.sleep(1_300)
      assert runners(runner_sup) == [grown]
    end

    # The floor runner never idles (restore stays on); a demand runner does. Kill
    # the floor while the demand runner keeps the pool at min, then let the
    # demand runner idle out: that exit is on purpose and is not replaced.
    @tag runner: [
           restore_min: true,
           min: 1,
           max: 2,
           max_concurrency: 1,
           idle_shutdown_after: 300,
           on_grow_start: &__MODULE__.report_grow_start/1
         ]
    test "a runner that idled out on purpose is not replaced",
         %{runner_sup: runner_sup, pool_pid: pool_pid} = config do
      listen(config)
      assert :sys.get_state(pool_pid).restore_min?
      [floor] = runners(runner_sup)

      # hold the floor runner's only slot, so the next call boots on demand
      parent = self()

      {:ok, _holder} =
        Task.start(fn ->
          FLAME.call(config.test, fn ->
            send(parent, :holding)
            Process.sleep(:infinity)
          end)
        end)

      assert_receive :holding, 2_000
      assert FLAME.call(config.test, fn -> :demand end) == :demand
      assert_receive {:grow_start, :demand}, 1_000
      [demand] = runners(runner_sup) -- [floor]

      Process.exit(floor, :kill)
      wait_for(fn -> runners(runner_sup) == [demand] end)

      ref = Process.monitor(demand)
      assert_receive {:DOWN, ^ref, :process, ^demand, {:shutdown, :idle}}, 2_000
      # a restore would boot within the 1 s debounce and report it
      refute_receive {:grow_start, :restore}, 1_500
      assert runners(runner_sup) == []
    end

    @tag runner: [
           restore_min: true,
           min: 1,
           max: 2,
           max_concurrency: 2,
           backend: :failing,
           on_grow_start: &__MODULE__.report_grow_start/1
         ]
    test "a failing restore backs off, then recovers", %{runner_sup: runner_sup} = config do
      listen(config)
      [victim] = runners(runner_sup)

      ExUnit.CaptureLog.capture_log(fn ->
        FailingBackend.fail_next(config.test, 100)
        Process.exit(victim, :kill)

        # attempts at about 1 s and 3 s (the wait doubles), not one a second
        Process.sleep(3_600)
        attempts = count_messages({:grow_start, :restore})
        assert attempts in 1..2

        FailingBackend.fail_next(config.test, 0)
        wait_for(fn -> length(runners(runner_sup)) == 1 end, 400)
      end)

      assert [replacement] = runners(runner_sup)
      assert replacement != victim
    end

    @tag runner: [restore_min: true, min: 1, max: 2, max_concurrency: 2, idle_shutdown_after: 100]
    test "the replacement is not shut down as idle", %{runner_sup: runner_sup} do
      [victim] = runners(runner_sup)
      Process.exit(victim, :kill)
      wait_for(fn -> match?([pid] when pid != victim, runners(runner_sup)) end)

      [replacement] = runners(runner_sup)
      ref = Process.monitor(replacement)
      refute_receive {:DOWN, ^ref, :process, ^replacement, _}, 500
    end
  end

  describe "max_concurrency :infinity" do
    @tag runner: [restore_min: true, min: 1, max: 1, max_concurrency: :infinity]
    test "a restore boots and the pool survives",
         %{runner_sup: runner_sup, pool_pid: pool_pid} = config do
      [victim] = runners(runner_sup)
      Process.exit(victim, :kill)
      wait_for(fn -> match?([pid] when pid != victim, runners(runner_sup)) end, 200)

      assert Process.whereis(config.test) == pool_pid
      assert FLAME.call(config.test, fn -> :works end) == :works
      assert Process.whereis(config.test) == pool_pid
    end

    @tag runner: [min: 0, max: 1, max_concurrency: :infinity]
    test "a demand boot from zero serves its callers", %{pool_pid: pool_pid} = config do
      tasks = for i <- 1..3, do: Task.async(fn -> FLAME.call(config.test, fn -> i end) end)
      assert Enum.map(tasks, &Task.await(&1, 5_000)) == [1, 2, 3]
      assert Process.whereis(config.test) == pool_pid
    end
  end

  describe "a min above max" do
    @tag runner: [restore_min: true, min: 3, max: 1, max_concurrency: 2]
    test "is restored only up to max", %{runner_sup: runner_sup} do
      for pid <- runners(runner_sup), do: Process.exit(pid, :kill)
      wait_for(fn -> length(runners(runner_sup)) == 1 end, 200)
      Process.sleep(1_300)
      assert length(runners(runner_sup)) == 1
    end
  end

  defp count_messages(message, count \\ 0) do
    receive do
      ^message -> count_messages(message, count + 1)
    after
      0 -> count
    end
  end
end
