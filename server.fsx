#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
open System.Security.Cryptography
open System
open Akka.Actor
open Akka.FSharp 
open Akka.Configuration
open System.Diagnostics

type ActorMsg =
    | WorkerMsg of string*int*int*int*int
    | WorkerMsgClient of string*int*int*int*int*int
    | DispatcherMsg of string*int
    | EndMsg
    | PrinterActor of string

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                transport-protocol = tcp
                port = 8777
                hostname = 10.20.142.27
            }
        }")

let proc = Process.GetCurrentProcess()
let cpu_time_stamp = proc.TotalProcessorTime
let mutable timer = new Stopwatch()

let system = ActorSystem.Create("Miner", configuration)


[<EntryPoint>]
let main args =
    let baseString = "rsaripalli;"
    let n = args.[0] |> int
    printfn "%d" n
    let mutable actN = ""
    for j in 1 .. n do
        actN <- actN + "0"
    printfn "%s" actN
    let rec appendAndIterate(bs: string, l: int, start: int, stop: int, N: int, myseq: array<string>) =
        if myseq.[0] = "" && l > 0 then 
            for i in start .. stop do
                if myseq.[0] = "" then 
                    let lessL = l - 1
                    let ebs = bs + string (char i)
                    appendAndIterate (ebs, lessL, start, stop, N, myseq)
                    if l = 1 then
                        let s = System.Text.Encoding.ASCII.GetBytes(ebs) |> (new SHA256Managed()).ComputeHash |> Array.map (fun (x : byte) -> System.String.Format("{0:x2}", x)) |> String.concat String.Empty
                        let firstN = s.[0 .. N-1]
                        // match firstN with
                        // | actN -> ebs 
                        if firstN.Equals(actN) then
                            // printfn "%s %s" ebs s
                            myseq.[0] <- (ebs + " " + s)
                        else
                            myseq.[0] <- ""

    let mutable count = 0;
    //worker actor
    let generateString (mailbox:Actor<_>)=
        let pref = system.ActorSelection("akka.tcp://Miner@10.20.142.27:8777/user/PrinterActor")
        let rec loop()=actor{
            let! msg = mailbox.Receive()
            match msg with 
            | WorkerMsg(bs,count, start, stop, N) -> let ans = [|""|]
                                                     appendAndIterate (bs, count, start, stop, N, ans)
                                                     printfn "%s" ans.[0]
                                                     mailbox.Sender()<! EndMsg //send back the finish message to boss
            | _ -> printfn "Worker Received Wrong message"
        }
        loop()
    
    let boss (mailbox:Actor<_>) =
        let start = 45
        let stop = 60
        let length = 7
        let totalactors =  (stop - start)
        let mid = (int (start + stop) / 2)
        let rec loop()=actor{
            let! msg = mailbox.Receive()
            match msg with 
            | DispatcherMsg(bs, N) ->
                                    let sref = system.ActorSelection("akka.tcp://RemoteMiner@10.20.142.27:8778/user/RemoteBossActor")
                                    sref <! (bs, length, start, mid+1, stop, N)
                                    let workerActorsPool = 
                                            [1 .. totalactors]
                                            |> List.map(fun id -> spawn system (sprintf "Local_%d" id) generateString)
                                    let workerenum = [|for lp in workerActorsPool -> lp|]
                                    let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))

                                    for i in start .. mid do
                                        let ebs = bs + string (char i)
                                        workerSystem <! WorkerMsg(ebs, length, start, stop, N)
            | EndMsg -> count <- count+1 //recieves end msg from worker
                                        //printfn "Worker %i finished its job" workerid
                        // printfn "%d" count
                        if count = totalactors then //checking if all workers have already sent the end message
                            mailbox.Context.System.Terminate() |> ignore //terminating the actor system
            | _ -> printfn "Dispatcher Received Wrong message"
            return! loop()
        }
        loop()

    //creating boss actor
    timer.Start()
    let bossRef = spawn system "boss" boss
    bossRef <! DispatcherMsg(baseString, n)
    let PrinterActor (mailbox:Actor<_>) = 
        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            if (message :? string) then
                if (string message) = "Done" then
                    let cpu_time = (proc.TotalProcessorTime-cpu_time_stamp).TotalMilliseconds
                    printfn "CPU time = %dms" (int64 cpu_time)
                    printfn "Absolute time = %dms" timer.ElapsedMilliseconds
                    bossRef <! EndMsg
                else
                    printfn "%s" (string message)
            return! loop()
        }
        loop()

    let printerRef = spawn system "PrinterActor" PrinterActor
    //waiting for boss actor to terminate the actor system
    system.WhenTerminated.Wait()

    0