defmodule FLAME.PoolFloorTest.FaultyBackend do
  # The local backend, except that while its counter is above zero a boot takes
  # one off and then fails (`:fail`) or never returns (`:hang`). The counter and
  # the mode live in `:persistent_term` under the `:fault_key` backend option.
  @behaviour FLAME.Backend

  alias FLAME.LocalBackend

  def arm(key, mode, n) when mode in [:fail, :hang] do
    ref = :counters.new(1, [:atomics])
    :counters.put(ref, 1, n)
    :persistent_term.put({__MODULE__, key}, {mode, ref})
  end

  def disarm(key), do: :persistent_term.erase({__MODULE__, key})

  @impl true
  def init(opts), do: LocalBackend.init(opts)

  @impl true
  def remote_spawn_monitor(state, term), do: LocalBackend.remote_spawn_monitor(state, term)

  @impl true
  def system_shutdown, do: LocalBackend.system_shutdown()

  @impl true
  def remote_boot(state) do
    case :persistent_term.get({__MODULE__, state.fault_key}, nil) do
      {mode, ref} ->
        if :counters.get(ref, 1) > 0 do
          :counters.sub(ref, 1, 1)
          fault(mode, state)
        else
          LocalBackend.remote_boot(state)
        end

      nil ->
        LocalBackend.remote_boot(state)
    end
  end

  defp fault(:fail, _state), do: {:error, {:unavailable, "SECRET_CANARY"}}

  defp fault(:hang, _state) do
    Process.sleep(:infinity)
  end
end

defmodule FLAME.PoolFloorTest do
  # Production, 2026-11-25: three floor runners took longer than the 120 s
  # boot timeout, `Task.async_stream` exited the pool inside `init/1`, and the
  # application that started the pool failed to boot.
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias FLAME.Pool
  alias FLAME.PoolFloorTest.FaultyBackend

  setup config do
    on_exit(fn -> FaultyBackend.disarm(config.test) end)
    {:ok, runner_sup: Module.concat(config.test, "RunnerSup")}
  end

  defp pool_opts(config, opts) do
    Keyword.merge(
      [
        name: config.test,
        backend: {FaultyBackend, fault_key: config.test},
        max: 4,
        max_concurrency: 2
      ],
      opts
    )
  end

  defp runners(runner_sup) do
    for {_, pid, :worker, [FLAME.Runner]} <- Supervisor.which_children(runner_sup), do: pid
  end

  defp wait_for(fun, tries \\ 150) do
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

  describe "a pool that restores min" do
    test "starts when a floor runner fails to boot, and restores it", config do
      FaultyBackend.arm(config.test, :fail, 1)

      log =
        capture_log(fn ->
          start_supervised!({Pool, pool_opts(config, restore_min: true, min: 3)})
          assert length(runners(config.runner_sup)) == 2
          wait_for(fn -> length(runners(config.runner_sup)) == 3 end)
        end)

      assert log =~ "started with 2 of 3 min runners; 1 failed to boot (exit)"
      refute log =~ "SECRET_CANARY"
      assert FLAME.call(config.test, fn -> :works end) == :works

      # never past min
      Process.sleep(1_300)
      assert length(runners(config.runner_sup)) == 3
    end

    test "starts when a floor runner outlasts boot_timeout, and stops that runner", config do
      FaultyBackend.arm(config.test, :hang, 1)

      log =
        capture_log(fn ->
          opts = pool_opts(config, restore_min: true, min: 2, boot_timeout: 300)
          start_supervised!({Pool, opts})
          # the hung runner is gone, not left booting outside the pool
          assert length(runners(config.runner_sup)) == 1
          wait_for(fn -> length(runners(config.runner_sup)) == 2 end)
        end)

      assert log =~ "started with 1 of 2 min runners; 1 failed to boot (timeout)"
      Process.sleep(1_300)
      assert length(runners(config.runner_sup)) == 2
      assert FLAME.call(config.test, fn -> :works end) == :works
    end

    test "starts with no runners when the whole floor fails", config do
      FaultyBackend.arm(config.test, :fail, 2)

      capture_log(fn ->
        start_supervised!({Pool, pool_opts(config, restore_min: true, min: 2)})
        assert runners(config.runner_sup) == []
        wait_for(fn -> length(runners(config.runner_sup)) == 2 end)
      end)

      assert FLAME.call(config.test, fn -> :works end) == :works
    end

    test "logs nothing when the floor boots", config do
      log =
        capture_log(fn ->
          start_supervised!({Pool, pool_opts(config, restore_min: true, min: 2)})
        end)

      assert length(runners(config.runner_sup)) == 2
      refute log =~ "failed to boot"
    end
  end

  describe "a pool that does not restore min" do
    test "still fails to start when a floor runner fails to boot", config do
      FaultyBackend.arm(config.test, :fail, 1)

      capture_log(fn ->
        assert {:error, _reason} = start_supervised({Pool, pool_opts(config, min: 2)})
      end)
    end
  end
end
