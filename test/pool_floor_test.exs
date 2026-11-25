defmodule FLAME.PoolFloorTest.FaultyBackend do
  # The local backend, except that while its counter is above zero a boot takes
  # one off and then fails (`:fail`) or never returns (`:hang`). The counter and
  # the mode live in `:persistent_term` under the `:fault_key` backend option.
  @behaviour FLAME.Backend

  alias FLAME.LocalBackend

  def arm(key, mode, n) when mode in [:fail, :hang, :time_out] do
    ref = :atomics.new(1, signed: true)
    :atomics.put(ref, 1, n)
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
        # Claimed in one step: the floor boots its runners concurrently.
        if :atomics.sub_get(ref, 1, 1) >= 0 do
          fault(mode, state)
        else
          LocalBackend.remote_boot(state)
        end

      nil ->
        LocalBackend.remote_boot(state)
    end
  end

  defp fault(:fail, _state), do: {:error, {:unavailable, "SECRET_CANARY"}}
  defp fault(:time_out, _state), do: {:error, :timeout}

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
        # the token stands in for the app key a real backend carries
        backend: {FaultyBackend, fault_key: config.test, token: "SECRET_CANARY"},
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
          wait_for(fn -> length(runners(config.runner_sup)) == 3 end)
        end)

      assert log =~ "started with 2 of 3 min runners; 1 failed to boot (exit)"
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
          wait_for(fn -> length(runners(config.runner_sup)) == 2 end)
        end)

      assert log =~ "started with 1 of 2 min runners; 1 failed to boot (timeout)"
      # the hung runner was stopped, not left booting beside its replacement
      Process.sleep(1_300)
      assert length(runners(config.runner_sup)) == 2
      assert FLAME.call(config.test, fn -> :works end) == :works
    end

    test "starts with no runners when the whole floor fails", config do
      FaultyBackend.arm(config.test, :fail, 2)

      capture_log(fn ->
        start_supervised!({Pool, pool_opts(config, restore_min: true, min: 2)})
        wait_for(fn -> length(runners(config.runner_sup)) == 2 end)
      end)

      assert FLAME.call(config.test, fn -> :works end) == :works
    end

    test "tells on_min_short how many runners booted and why the rest did not", config do
      FaultyBackend.arm(config.test, :fail, 2)
      parent = self()

      capture_log(fn ->
        opts =
          pool_opts(config,
            restore_min: true,
            min: 3,
            on_min_short: fn info -> send(parent, {:short, info}) end
          )

        start_supervised!({Pool, opts})
      end)

      assert_received {:short, %{name: name, count: 1, min: 3, failures: [:exit, :exit]}}
      assert name == config.test
    end

    test "a failing on_min_short does not stop the start", config do
      FaultyBackend.arm(config.test, :fail, 1)

      log =
        capture_log(fn ->
          opts = pool_opts(config, restore_min: true, min: 1, on_min_short: fn _ -> raise "x" end)
          start_supervised!({Pool, opts})
          wait_for(fn -> length(runners(config.runner_sup)) == 1 end)
        end)

      assert log =~ "on_min_short raised RuntimeError"
    end

    # A demand boot right after a short start would otherwise take the missing
    # slot as a runner that idles out, and nothing would put the floor back.
    test "boots the missing runners at once, so a caller does not take their place",
         config do
      FaultyBackend.arm(config.test, :fail, 1)
      parent = self()

      capture_log(fn ->
        opts =
          pool_opts(config,
            restore_min: true,
            min: 1,
            idle_shutdown_after: 100,
            on_grow_start: fn info -> send(parent, {:grow_start, info.reason}) end
          )

        start_supervised!({Pool, opts})
        # before the restore's 1 s debounce would have fired, and before any call
        assert_receive {:grow_start, :restore}, 500
        assert FLAME.call(config.test, fn -> :works end) == :works
      end)

      refute_received {:grow_start, :demand}

      Process.sleep(600)
      assert length(runners(config.runner_sup)) == 1
    end

    # The floor boot and its immediate replacement both fail. Neither the
    # pool's line nor a boot task's crash report carries the backend's reason.
    test "never logs a failed boot's reason, the replacement's included", config do
      FaultyBackend.arm(config.test, :fail, 2)

      log =
        capture_log(fn ->
          start_supervised!({Pool, pool_opts(config, restore_min: true, min: 1)})
          wait_for(fn -> length(runners(config.runner_sup)) == 1 end, 250)
        end)

      assert log =~ "started with 0 of 1 min runners"
      refute log =~ "SECRET_CANARY"
    end

    # The floor hangs, then its replacement hangs: the replacement has the floor's
    # deadline, so it fails. The waiting caller's demand boot, after the 1 s
    # demand debounce and before the 2 s restore backoff, fills the floor.
    test "a replacement that never answers is stopped, and a caller still gets a runner",
         config do
      FaultyBackend.arm(config.test, :hang, 2)

      capture_log(fn ->
        opts = pool_opts(config, restore_min: true, min: 1, boot_timeout: 300)
        start_supervised!({Pool, opts})
        assert FLAME.call(config.test, fn -> :works end, timeout: 10_000) == :works
      end)

      # the hung runners were stopped, not left booting beside the one that works
      assert length(runners(config.runner_sup)) == 1
    end

    # With no caller, nothing but the restore can fill the floor: the hung
    # replacement fails at the deadline and a second restore boots it.
    test "a replacement that never answers is retried by the restore", config do
      FaultyBackend.arm(config.test, :hang, 2)
      parent = self()

      capture_log(fn ->
        opts =
          pool_opts(config,
            restore_min: true,
            min: 1,
            boot_timeout: 300,
            on_grow_start: fn info -> send(parent, {:grow_start, info.reason}) end
          )

        start_supervised!({Pool, opts})
        assert_receive {:grow_start, :restore}, 500
        # 300 ms hung, then the 2 s backoff
        assert_receive {:grow_start, :restore}, 4_000
        wait_for(fn -> length(runners(config.runner_sup)) == 1 end, 100)
      end)

      refute_received {:grow_start, :demand}
    end

    # The floor, its replacement and the waiting caller's demand boot all hang.
    # The demand boot stands in for the floor runner, so the restore counts it
    # and schedules nothing: without the floor's deadline it would hold the slot
    # forever and the floor would never come back.
    test "a demand boot standing in for the floor gets the floor's deadline", config do
      FaultyBackend.arm(config.test, :hang, 3)
      parent = self()

      capture_log(fn ->
        opts =
          pool_opts(config,
            restore_min: true,
            min: 1,
            boot_timeout: 300,
            on_grow_start: fn info -> send(parent, {:grow_start, info.reason}) end
          )

        start_supervised!({Pool, opts})
        assert FLAME.call(config.test, fn -> :works end, timeout: 10_000) == :works
      end)

      assert_received {:grow_start, :demand}
      assert length(runners(config.runner_sup)) == 1
    end

    # A caller boots above the floor while the floor runner is busy, and that boot
    # hangs. The floor runner then dies: the restore counts the hung boot, so only
    # its deadline frees the slot for the restore to fill.
    test "a hung boot above the floor does not keep the floor from coming back",
         config do
      parent = self()

      capture_log(fn ->
        opts =
          pool_opts(config,
            restore_min: true,
            min: 1,
            boot_timeout: 300,
            on_grow_start: fn info -> send(parent, {:grow_start, info.reason}) end
          )

        start_supervised!({Pool, opts})
        [floor] = runners(config.runner_sup)

        # fill the floor runner's two slots
        for _ <- 1..2 do
          spawn(fn ->
            FLAME.call(config.test, fn -> Process.sleep(:infinity) end, timeout: 60_000)
          end)
        end

        Process.sleep(200)
        FaultyBackend.arm(config.test, :hang, 1)
        spawn(fn -> catch_exit(FLAME.call(config.test, fn -> :ok end, timeout: 1_000)) end)
        assert_receive {:grow_start, :demand}, 2_000

        Process.exit(floor, :kill)
        assert_receive {:grow_start, :restore}, 4_000
        wait_for(fn -> length(runners(config.runner_sup)) == 1 end, 100)
      end)
    end

    # The floor and its replacement fail, and a grow lands before the restore's
    # backoff and hangs. The grow is counted by the restore, so it has the deadline.
    test "a hung grow below the floor does not keep the floor from coming back",
         config do
      FaultyBackend.arm(config.test, :hang, 3)
      parent = self()

      capture_log(fn ->
        opts =
          pool_opts(config,
            restore_min: true,
            min: 1,
            boot_timeout: 300,
            on_grow_start: fn info -> send(parent, {:grow_start, info.reason}) end
          )

        start_supervised!({Pool, opts})
        assert_receive {:grow_start, :restore}, 500
        Process.sleep(400)
        spawn(fn -> Pool.grow(config.test, 100) end)
        assert_receive {:grow_start, :grow}, 500
        # the grow fails at 300 ms, then the restore boots the floor
        assert_receive {:grow_start, :restore}, 4_000
        wait_for(fn -> length(runners(config.runner_sup)) == 1 end, 100)
      end)
    end

    test "a grow that fails replies with the boot's own exit reason", config do
      FaultyBackend.arm(config.test, :fail, 1)
      start_supervised!({Pool, pool_opts(config, restore_min: true, min: 0)})

      capture_log(fn ->
        assert {:error, {{:shutdown, {:unavailable, _}}, {GenServer, :call, _}}} =
                 Pool.grow(config.test)
      end)
    end

    test "counts a backend that returns a timeout as a timeout", config do
      FaultyBackend.arm(config.test, :time_out, 1)
      parent = self()

      capture_log(fn ->
        opts =
          pool_opts(config,
            restore_min: true,
            min: 1,
            on_min_short: fn info -> send(parent, {:short, info.failures}) end
          )

        start_supervised!({Pool, opts})
      end)

      assert_received {:short, [:timeout]}
    end

    # The replacement fails too, so a waiting caller boots on demand. That
    # runner fills the missing slot and must not idle out of it.
    test "a demand boot that fills a missing min runner does not idle out", config do
      FaultyBackend.arm(config.test, :fail, 2)

      capture_log(fn ->
        # idle longer than the restore's doubled 2 s wait, so the restore sees the
        # demand runner and boots nothing; the idle exit would then empty the pool
        opts = pool_opts(config, restore_min: true, min: 1, idle_shutdown_after: 3_000)
        start_supervised!({Pool, opts})
        assert FLAME.call(config.test, fn -> :works end, timeout: 5_000) == :works
      end)

      Process.sleep(7_000)
      assert length(runners(config.runner_sup)) == 1
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
        assert {:error, {{:shutdown, {:failed_to_start_child, _id, failure}}, _child_spec}} =
                 start_supervised({Pool, pool_opts(config, min: 2)})

        # the child spec, which ExUnit adds, carries the options; the failure must not
        refute inspect(failure) =~ "SECRET_CANARY"
      end)
    end
  end
end
