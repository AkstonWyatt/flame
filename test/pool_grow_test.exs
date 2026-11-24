defmodule FLAME.PoolGrowTest do
  use ExUnit.Case, async: true

  alias FLAME.Pool

  setup config do
    runner_sup = Module.concat(config.test, "RunnerSup")
    opts = Keyword.merge(config.runner, name: config.test)
    pool_pid = start_supervised!({Pool, opts})
    {:ok, runner_sup: runner_sup, pool_pid: pool_pid}
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
      assert Process.alive?(config.pool_pid)
    end
  end

  describe "restoring min" do
    @tag runner: [min: 2, max: 3, max_concurrency: 2]
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

    @tag runner: [min: 1, max: 3, max_concurrency: 2]
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

    @tag runner: [min: 1, max: 2, max_concurrency: 2, idle_shutdown_after: 100]
    test "the replacement is not shut down as idle", %{runner_sup: runner_sup} do
      [victim] = runners(runner_sup)
      Process.exit(victim, :kill)
      wait_for(fn -> match?([pid] when pid != victim, runners(runner_sup)) end)

      [replacement] = runners(runner_sup)
      ref = Process.monitor(replacement)
      refute_receive {:DOWN, ^ref, :process, ^replacement, _}, 500
    end
  end
end
