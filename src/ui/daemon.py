import asyncio
import threading
import logging
from typing import Optional, List, Tuple

from telemetry_bus.events import GovernanceEvent
from telemetry_bus.event_bus import EventBus, Topic
from recon_operator.state_store import StateStore
from recon_operator.control_loop import ControlLoop

log = logging.getLogger(__name__)

class GaCDaemon:
    """
    Runs the 4-plane asynchronous architecture inside a dedicated daemon thread.
    Exposes thread-safe, synchronous methods for the UI (Facade layer).
    """
    def __init__(self, desired_state_dir: str):
        self.desired_state_dir = desired_state_dir
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._bus: Optional[EventBus] = None
        self._store: Optional[StateStore] = None
        self._control_loop: Optional[ControlLoop] = None
        
        self._stop_event = threading.Event()
        
        # Thread-safe event history for the UI's "next step" feature
        self._history_lock = threading.Lock()
        self._event_history: List[GovernanceEvent] = []
        
    def start(self):
        """Starts the daemon thread and the asyncio loop."""
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="GaCDaemon")
        self._thread.start()

    def stop(self):
        """Stops the daemon thread gracefully."""
        self._stop_event.set()
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=2.0)

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # --- Thread-Safe UI Methods (Facade Delegates) ---
    
    def get_events(self) -> List[GovernanceEvent]:
        """Returns a snapshot of captured events."""
        with self._history_lock:
            return list(self._event_history)
            
    def get_actual_state_snapshot(self) -> dict:
        """Returns a copy of the actual state."""
        if not self._store:
            return {}
        # Assuming StateStore has a thread-safe or dictionary-copying snapshot method
        # We will wrap it safely or just call snapshot()
        return self._store.snapshot()
        
    def publish_event(self, event: GovernanceEvent):
        """Allows the UI to trigger events (e.g., from Playground)."""
        if not self._loop or not self._bus:
            return
            
        asyncio.run_coroutine_threadsafe(self._bus.publish(event), self._loop)

    # --- Internal Background Loop ---

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        
        try:
            self._loop.run_until_complete(self._async_main())
        except Exception as e:
            log.error(f"GaCDaemon crashed: {e}", exc_info=True)
        finally:
            # Clean up
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()

    async def _async_main(self):
        async with EventBus() as bus:
            self._bus = bus
            self._store = StateStore(bus)
            await self._store.start()
            
            self._control_loop = ControlLoop(
                self._store, bus, 
                desired_state_dir=self.desired_state_dir,
                tick_interval_s=1.0, # UI updates every second
            )
            
            # Start background subscribers for UI Event History
            capture_task = asyncio.create_task(self._capture_all_events(bus))
            
            # Start control loop in background
            cl_task = asyncio.create_task(self._control_loop.run())
            
            # Keep alive until stopped
            while not self._stop_event.is_set():
                await asyncio.sleep(0.5)
                
            cl_task.cancel()
            capture_task.cancel()
            await self._store.stop()

    async def _capture_all_events(self, bus: EventBus):
        """Captures all events into a thread-safe list for the UI flow."""
        try:
            # We want to subscribe to multiple topics
            async def sub(topic: Topic):
                async for e in bus.subscribe(topic, subscriber_id=f"ui_{topic.name}"):
                    with self._history_lock:
                        self._event_history.append(e)

            tasks = [asyncio.create_task(sub(t)) for t in Topic]
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
