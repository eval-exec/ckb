use crate::{
    broadcast_exit_signals, new_crossbeam_exit_rx, new_tokio_exit_rx, register_thread,
    wait_all_ckb_services_exit,
};
use ckb_async_runtime::{new_global_runtime, Handle};
use ckb_channel::select;
use rand::Rng;
use std::time::Duration;

fn send_ctrlc_later(duration: Duration) {
    std::thread::spawn(move || {
        std::thread::sleep(duration);
        // send SIGINT to myself
        unsafe {
            libc::raise(libc::SIGINT);
            println!("[ $$ sent SIGINT to myself $$ ]");
        }
    });
}

fn start_many_threads() {
    for i in 0..5 {
        let join = std::thread::spawn(move || {
            let ticker = ckb_channel::tick(Duration::from_millis(500));
            let deadline = ckb_channel::after(Duration::from_millis(
                (rand::thread_rng().gen_range(1.0..5.0) * 1000.0) as u64,
            ));

            let stop = new_crossbeam_exit_rx();

            loop {
                select! {
                    recv(ticker) -> _ => {
                        println!("thread {} received tick signal", i);
                    },
                    recv(stop) -> _ => {
                        println!("thread {} received crossbeam exit signal", i);
                        return;
                    },
                    recv(deadline) -> _ =>{
                        println!("thread {} finish its job", i);
                        return
                    }
                }
            }
        });
        register_thread("test thread", join);
    }
}

fn start_many_tokio_tasks(handle: Handle) {
    for i in 0..5 {
        let stop = new_tokio_exit_rx();

        handle.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            let duration =
                Duration::from_millis((rand::thread_rng().gen_range(1.0..5.0) * 1000.0) as u64);
            let deadline = tokio::time::sleep(duration);
            tokio::pin!(deadline);

            loop {
                tokio::select! {
                    _ = &mut deadline =>{
                        println!("tokio task {} finish its job", i);
                        break;
                    }
                    _ = interval.tick()=> {
                        println!("tokio task {} received tick signal", i);
                    },
                    _ = stop.cancelled() => {
                        println!("tokio task {} receive exit signal", i);
                        break
                    },
                    else => break,
                }
            }
        });
    }
}

#[test]
fn basic() {
    let (handle, stop_recv, _runtime) = new_global_runtime();

    ctrlc::set_handler(move || {
        broadcast_exit_signals();
    })
    .expect("Error setting Ctrl-C handler");

    send_ctrlc_later(Duration::from_secs(3));

    start_many_threads();
    start_many_tokio_tasks(handle);

    wait_all_ckb_services_exit();
}
