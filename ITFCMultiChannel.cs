using ITFCProxy;
using Microsoft.SystemCenter.Orchestrator.Integration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

/// <summary>
/// Class created by Tiago de Souza Oliveira
/// Date: 26 October de 2016 01:33

/// This class decorated by the Orchestrator SDK allows it to be installed in Farm Orchestrator
/// 
/// It is endowed with written audit and error logs as well as telemetry in the Machine's Performance Counters
/// Performs multi-channel requests (parallel execution and serial execution channels within each channel) with end-time limiters and number of items to process per channel
/// 
/// Before running this DLL inside the Orchestrator, ensure that on all Orch farm servers, PerfCounter has been created (powershell script commented below)
/// 
/// Activity responsible for triggering SOAP requests for ITFC Web Services via .Net Parallels.
/// </summary>
namespace Orchestrator_ITFC_ParallelActivity
{
    //Parallel Programming in the .NET Framework
    //https://msdn.microsoft.com/en-us/library/dd460693(v=vs.110).aspx

    [Activity("ITFC Multi Channel Request")]
    public class ITFCMultiChannel
    {
        public enum ProcessStatusMultiChannel { Success, AllError, PartialError };

        enum WSAction { Request, Response };

        #region private declarations
        private ProcessStatusMultiChannel _processStatus = ProcessStatusMultiChannel.Success;

        private int _requestExecutedTotal = 0;

        private int _responseReturnTotal = 0;

        private int _totalTimeExecution = 0;

        private int _workerThreads = 0;

        private int _portThreads = 0;

        private int _timeout = 3;

        private int _channelNumber = 1;

        private string _url = "http://contoso.com/aia/Service.svc";

        private string _interfaceName = string.Empty;

        private string _stepName = string.Empty;

        private bool _auditLog = false;

        private DateTime _endTimeExecution = DateTime.MinValue;

        private int _queueOrCycleSize = 0;

        private string _runbookServerName = string.Empty;

        private string _logPathFile = @"C:\temp\";
        #endregion private declarations

        #region properties
        [ActivityInput("Timeout (sec)")]
        public int TimeOut
        {
            set { _timeout = value; }
        }

        [ActivityInput("Channel number")]
        public int ChannelNumber
        {
            set { _channelNumber = value; }
        }

        [ActivityInput("URL (.svc)")]
        public string URL
        {
            set { _url = value; }
        }

        [ActivityInput("Interface Name")]
        public string InterfaceName
        {
            set { _interfaceName = value; }
        }

        [ActivityInput("Step Name")]
        public string StepName
        {
            set { _stepName = value; }
        }

        [ActivityInput("Audit Log flag")]
        public bool AuditLog
        {
            set { _auditLog = value; }
        }

        [ActivityInput("End time for execution")]
        public DateTime EndTimeExecution
        {
            set { _endTimeExecution = value; }
        }

        [ActivityInput("Queue Size")]
        public int QueueOrCycleSize
        {
            set { _queueOrCycleSize = value; }
        }

        [ActivityInput("Runbook Server Name")]
        public string RunbookServerName
        {
            set { _runbookServerName = value; }
        }

        [ActivityInput("Pathfile directory")]
        public string LogPathfile
        {
            set { _logPathFile = value; }
        }

        [ActivityOutput("Process Status")]
        public ProcessStatusMultiChannel ProcessStatus
        {
            get
            {
                return RunMultiChannell();
            }
        }

        [ActivityOutput("Request Executed Total")]
        public int RequestExecutedTotal
        {
            get
            {
                return _requestExecutedTotal;
            }
        }

        [ActivityOutput("Response Return Total")]
        public int ResponseReturnTotal
        {
            get
            {
                return _responseReturnTotal;
            }
        }

        [ActivityOutput("Total Time Execution (sec)")]
        public int TotalTimeExecution
        {
            get
            {
                return _totalTimeExecution;
            }
        }
        #endregion properties

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ProcessStatusMultiChannel RunMultiChannell()
        {
            StringBuilder sberroraudit = new StringBuilder();

            Guid guidParent = Guid.NewGuid();

            int splitmsgcyclesupper = 0;

            Stopwatch timeTotal = new Stopwatch();

            timeTotal.Start();

            #region adjusting ThreadPool
            //Obtendo disponibilidade de threads
            System.Threading.ThreadPool.GetAvailableThreads(out _workerThreads, out _portThreads);

            //Limitando o pedido de threads pela quantidade disponível
            _channelNumber = (_channelNumber > _workerThreads) ? _workerThreads : _channelNumber;

            System.Threading.ThreadPool.SetMinThreads(_channelNumber, _channelNumber);
            #endregion ajustando ThreadPool

            #region calculating message distribution per channel
            decimal splitmsgscycles = decimal.Divide(_queueOrCycleSize, _channelNumber);
            splitmsgcyclesupper = (Int32)(Math.Ceiling(splitmsgscycles));
            #endregion

            //number of channels to open to switch to Parallel
            var channells = Enumerable.Range(1, _channelNumber);

            Parallel.ForEach
                (
                channells,
                new ParallelOptions { MaxDegreeOfParallelism = -1 },
                item => ProcessChannell(guidParent, item, splitmsgcyclesupper, ref sberroraudit)
                );

            //Consolidated cycles
            TimeSpan ts = timeTotal.Elapsed;

            string tempototalrequisicaoHMSM = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);

            _totalTimeExecution = (int)ts.TotalSeconds;

            #region Consolidated log 
            string msgcons = string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}",
                guidParent.ToString(),
                string.Empty,
                string.Empty,
                _interfaceName,
                _stepName,
                tempototalrequisicaoHMSM,
                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                _workerThreads,
                _portThreads,
                _channelNumber,
                _queueOrCycleSize,
                0,
                _runbookServerName,
                string.Empty
                );
            #endregion log consolidado

            sberroraudit.AppendLine(msgcons);

            #region Log file
            string fullfilepath = string.Format(@"{0}LogsServiceRequester_{1}{2}.csv", _logPathFile, _interfaceName, _stepName);

            using (StreamWriter sw = File.AppendText(fullfilepath))
            {
                sw.WriteLine(sberroraudit.ToString().Trim());
            }
            #endregion Log file

            return ProcessStatusMultiChannel.Success;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ID"></param>
        /// <param name="guidParent"></param>
        /// <param name="channell"></param>
        /// <param name="totalCycles"></param>
        private void ProcessChannell(Guid guidParent, int channell, int totalCycles, ref StringBuilder sb)
        {
            int counter = 0;

            //If necessary, use this constructor's overload to pass parameters in the WebServices header
            System.ServiceModel.EndpointAddress remoteAddress = new System.ServiceModel.EndpointAddress(_url);

            //creating an HTTP connection per channel
            //This is done so that the F5 understands a connection to smaller groups of request.
            //It is from distinct connections that F5 performs load distribution on balanced nodes.
            using (RequestProcessorClient itfc = new RequestProcessorClient(new System.ServiceModel.BasicHttpBinding(), remoteAddress))
            {
                //Setting the requisition timeout
                itfc.Endpoint.Binding.SendTimeout = new TimeSpan(0, 0, 0, _timeout);

                //Do not exceed request timeout
                while (_endTimeExecution > DateTime.Now)
                {
                    if (totalCycles > counter)
                    {
                        Guid guidChild = Guid.NewGuid();

                        ExecutionStatus response = ExecutionStatus.NotExecuted;

                        string timeRequestHMSM = string.Empty;

                        try
                        {
                            //Informing PerfCounter of request
                            IncrementCounter(WSAction.Request, channell);

                            Stopwatch timeRequest = new Stopwatch();

                            string mensagemLog = string.Empty;

                            _requestExecutedTotal++;

                            timeRequest.Start();

                            //Running the request against the server
                            response = itfc.ExecuteStep(guidChild, _interfaceName, _stepName);

                            TimeSpan tsspan = timeRequest.Elapsed;

                            _responseReturnTotal++;

                            timeRequestHMSM = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", tsspan.Hours, tsspan.Minutes, tsspan.Seconds, tsspan.Milliseconds / 10);

                            //Informing PerfCounter of the request response
                            IncrementCounter(WSAction.Response, channell);

                            #region Msg Log Error and Audit
                            mensagemLog = string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}",
                                    guidParent.ToString(),
                                    response.ToString(),
                                    guidChild.ToString(),
                                    _interfaceName,
                                    string.Format("{0}-{1}", _stepName, channell),
                                    timeRequestHMSM,
                                    DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                                    _workerThreads,
                                    _portThreads,
                                    _channelNumber,
                                    _queueOrCycleSize,
                                    totalCycles,
                                    _runbookServerName,
                                    string.Empty
                                    );
                            #endregion Msg Log Erro and Audit

                            //If the request is not successful, error logging
                            if (response != ExecutionStatus.Success)
                            {

                                sb.AppendLine(mensagemLog);
                            }

                            //If auditing is turned on, write same success log
                            if (_auditLog && response == ExecutionStatus.Success)
                            {
                                sb.AppendLine(mensagemLog);
                            }
                        }
                        catch (TimeoutException te)
                        {
                            //Contains at least 1 error the requisition batch
                            ///TODO
                            ///Calculate when it is total error
                            _processStatus = ProcessStatusMultiChannel.PartialError;

                            #region Log exception

                            string exception = string.Format("Timeout exception\n\n", te.Message);

                            string msgerr = string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}",
                                guidParent.ToString(),
                                response.ToString(),
                                guidChild.ToString(),
                                _interfaceName,
                                string.Format("{0}-{1}", _stepName, channell),
                                timeRequestHMSM,
                                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                                _workerThreads,
                                _portThreads,
                                _channelNumber,
                                _queueOrCycleSize,
                                totalCycles,
                                _runbookServerName,
                                exception
                                );
                            #endregion Log exception

                            sb.AppendLine(msgerr);
                        }
                        catch (AggregateException ae)
                        {
                            //Contains at least 1 error the requisition batch
                            ///TODO
                            ///Calculate when it is total error
                            _processStatus = ProcessStatusMultiChannel.PartialError;

                            #region Log exception

                            string exception = string.Format("Parallel.ForEach exception\n\n", ae.Message);

                            string msgerr = string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}",
                                guidParent.ToString(),
                                response.ToString(),
                                guidChild.ToString(),
                                _interfaceName,
                                string.Format("{0}-{1}", _stepName, channell),
                                timeRequestHMSM,
                                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                                _workerThreads,
                                _portThreads,
                                _channelNumber,
                                _queueOrCycleSize,
                                totalCycles,
                                _runbookServerName,
                                exception
                                );
                            #endregion Log exception

                            sb.AppendLine(msgerr);
                        }
                        catch (WebException wexc)
                        {
                            //Contains at least 1 error the requisition batch
                            ///TODO
                            ///Calculate when it is total error
                            _processStatus = ProcessStatusMultiChannel.PartialError;

                            #region Log exception
                            string exception = string.Format("HTTPCode:{0} \n\n Description:{1} \n\n EMessage:{2}",
                                    ((HttpWebResponse)wexc.Response).StatusCode.ToString(),
                                    ((HttpWebResponse)wexc.Response).StatusDescription.ToString(),
                                    wexc.Message
                                );

                            string msgerr = string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}",
                                guidParent.ToString(),
                                response.ToString(),
                                guidChild.ToString(),
                                _interfaceName,
                                string.Format("{0}-{1}", _stepName, channell),
                                timeRequestHMSM,
                                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                                _workerThreads,
                                _portThreads,
                                _channelNumber,
                                _queueOrCycleSize,
                                totalCycles,
                                _runbookServerName,
                                exception
                                );
                            #endregion Log exception

                            sb.AppendLine(msgerr);
                        }
                        catch (Exception exc)
                        {
                            //Contains at least 1 error the requisition batch
                            ///TODO
                            ///Calculate when it is total error
                            _processStatus = ProcessStatusMultiChannel.PartialError;

                            #region Log exception

                            string msgerr = string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}",
                                guidParent.ToString(),
                                response.ToString(),
                                guidChild.ToString(),
                                _interfaceName,
                                string.Format("{0}-{1}", _stepName, channell),
                                timeRequestHMSM,
                                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                                _workerThreads,
                                _portThreads,
                                _channelNumber,
                                _queueOrCycleSize,
                                totalCycles,
                                _runbookServerName,
                                exc.Message
                                );
                            #endregion Log exception

                            sb.AppendLine(msgerr);
                        }
                    }
                    else
                    {
                        return;
                    }

                    counter++;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="action"></param>
        /// <param name="channell"></param>
        private void IncrementCounter(WSAction action, int channell)
        {
            PerformanceCounter performanceCounter = null;
            PerformanceCounter totalPerformanceCounter = null;

            try
            {
                string instanceNameMChannell = string.Format("{0}_{1}_{2}_{3}", _interfaceName, _stepName, action, channell);

                //Counter to get total for each interface / step / request-response / execution channel
                performanceCounter = new PerformanceCounter("ITFC Orch Call Counter", "Call/Sec", instanceNameMChannell, false);
                performanceCounter.Increment();

                if (action == WSAction.Request)
                {
                    string instanceName = string.Format("Call Total - {0}_{1}", _interfaceName, _stepName);

                    //Counter for all interfaces / steps
                    totalPerformanceCounter = new PerformanceCounter("ITFC Orch Call Counter", "Call/Sec", instanceName, false);
                    totalPerformanceCounter.Increment();


                    //Counter for all interfaces / steps
                    totalPerformanceCounter = new PerformanceCounter("ITFC Orch Call Counter", "Call/Sec", "Call Total", false);
                    totalPerformanceCounter.Increment();
                }
            }
            catch (Exception exc)
            {
                WriteLogError(exc);
            }
            finally
            {
                if (performanceCounter != null)
                {
                    performanceCounter.Dispose();
                }

                if (totalPerformanceCounter != null)
                {
                    totalPerformanceCounter.Dispose();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="exc"></param>
        private void WriteLogError(Exception exc)
        {
            #region Log file
            #region log general
            string msgerro = string.Format("'{0}','{1}','{2}','{3}','{4}','{5}','{6}'",
                string.Empty,
                string.Empty,
                string.Empty,
                string.Empty,
                string.Empty,
                string.Empty,
                DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                exc.Message
                );
            #endregion log consolidado

            string fullfilepath = string.Format(@"{0}LogsServiceRequester_{1}{2}.csv", _logPathFile, _interfaceName, _stepName);

            using (StreamWriter sw = File.AppendText(fullfilepath))
            {
                sw.WriteLine(msgerro);
            }
            #endregion Log file
        }

    }
}
