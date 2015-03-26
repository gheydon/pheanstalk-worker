<?php

namespace Pheanstalk;

/**
 * Default implementation of a php worker.
 *
 * @author Gordon Heydon
 * @package Pheanstalk Worker
 * @licence http://www.opensource.org/licenses/mit-license.php
 */

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Worker
{
    private $_pheanstalk;
    private $_callbacks = array();
    private $_logger = null;

    /**
     * @param string $host
     * @param int $port
     * @param int $connectTimeout
     * @param \Psr\Log\LoggerInterface $logger
     */
    public function __construct($host, $port = PheanstalkInterface::DEFAULT_PORT, $connectTimeout = null, LoggerInterface $logger = null)
    {
        $this->_pheanstalk = new Pheanstalk($host, $port, $connectTimeout);
        if ($logger) {
            $this->_logger = $logger;
        }
        else {
            $this->_logger = new NullLogger();
        }
        $this->_logger->notice('Worker initiated.');
    }

    /**
     * @param $tube
     * @param callable $callable
     * @param string $onError
     */
    public function register($tube, callable $callable, $retryOn = '')
    {
        $this->_callbacks[$tube] = array(
            'callable' => $callable,
            'retryOn' => $retryOn,
        );
        $this->_pheanstalk->watch($tube);
        $this->_logger->notice('Callback registered', array('tube' => $tube));
    }

    /**
     * Process jobs forever.
     */
    public function process()
    {
        $this->_logger->notice('Start processing jobs', array('tubes' => array_keys($this->_callbacks)));
        while (1) {
            $this->processOne();
        }
    }

    /**
     * Reserve the next job and process.
     *
     * @param $timeout
     *  Sets how long the reserve process will wait for a job.
     * @return bool|object|Job
     *  returns the job which was just processed.
     * @throws Exception\WorkerException
     *  if a job is reserved which has no registered callable then throw this error and stop the processing. This should
     *  never occur as we are only watching tubes with registered handlers.
     */
    public function processOne($timeout = null)
    {
        $job = $this->_pheanstalk->reserve($timeout);
        // Only process the job if a job is returned.
        if ($job) {
            // Get the job stats so we know which tube this was received from.
            $statJob = $this->_pheanstalk->statsJob($job);
            $tube = $statJob['tube'];

            if (isset($this->_callbacks[$tube])) {
                try {
                    // get  starting stats for later comparision.
                    $startTime = microtime(TRUE);
                    $startMem = memory_get_usage();
                    $this->_callbacks[$tube]['callable']($job);
                    $this->_pheanstalk->delete($job);
                    $this->_logger->notice('Job ' . $job->getId() . ' complete. Time taken: ' . (microtime(TRUE) - $startTime) . ' Memory Used: ' . (memory_get_usage() - $startMem));
                } catch (Exception $e) {
                    if (!empty($this->_callbacks[$tube]['retryOn']) && is_a($e,
                        $this->_callbacks['retryOn'])
                    ) {
                        $this->_logger->warning('Job ' . $job->getId() . ' failed. Releasing Job and retrying again.', array('trace' => $e->getTraceAsString()));
                        $this->_pheanstalk->release($job);
                    } else {
                        $this->_logger->error('Job ' . $job->getId() . ' failed. Burying job.', array('trace' => $e->getTraceAsString()));
                        $this->_pheanstalk->bury($job);
                    }
                }
            } elseif ($tube == "default") {
                // if we receive a job from the "default" queue and there is no registered function for the default queue then ignore it and move on.
                $this->_pheanstalk->release($job);
                $this->_pheanstalk->ignore('default');
                $this->_logger->warning('Job reserved from default tube. Releasing job and ignoring default tube.', $statJob);
            } else {
                // we know nothing about this job and what we should do with it. We should not have received this so something is really not right.
                $this->_pheanstalk->release($job);
                $this->_logger->error("Job fetched for unknown tube '$tube'", array('id' => $job->getId()));
                throw new Exception\WorkerException(sprintf(
                    'Job fetched for unknown tube "%s"',
                    $tube
                ));
            }
        }

        return $job;
    }
}