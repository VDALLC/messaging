<?php
namespace Vda\Messaging\Stomp\Client\PeclStomp;

use Vda\Messaging\MessagingException;
use Vda\Messaging\Message;
use Vda\Messaging\Stomp\Client\IStompClient;
use Vda\Messaging\Stomp\StompMessage;
use Vda\Messaging\Subscription;

/**
 * Interface adaptor for PECL Stomp extension
 */
class PeclStompClient implements IStompClient
{
    const ERROR_UNEXPECTED_EOF = 1; // kick out from server (timeout)

    private static $errorCodes = array(
        'Unexpected EOF while reading from socket' => self::ERROR_UNEXPECTED_EOF,
    );

    private static $ackModes = array(
        Subscription::ACK_AUTO => 'auto',
        Subscription::ACK_INDIVIDUAL => 'client-individual',
        Subscription::ACK_CUMULATIVE => 'client',
    );
    /**
     * @var \Stomp
     */
    private $stomp = null;
    private $readTimeout = 5;
    private $brokerAddress;
    private $username;
    private $password;
    private $clientId;

    public function __construct(
        $brokerAddress,
        $clientId = null,
        $username = null,
        $password = null
    ) {
        if (!class_exists('\Stomp')) {
            throw new \RuntimeException(
                "Unable to load the PECL stomp implementation. Please install the dev-php/pecl-stomp package."
            );
        }

        $this->brokerAddress = $brokerAddress;
        $this->username = $username;
        $this->password = $password;
        $this->clientId = $clientId;
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    public function connect(array $headers = array())
    {
        try {
            $this->disconnect();

            $this->stomp = new \Stomp(
                $this->brokerAddress,
                $this->username,
                $this->password,
                $headers
            );
        } catch (\StompException $e) {
            throw new MessagingException("Unable to connect to '{$this->brokerAddress}'", 0, $e);
        }
    }

    public function disconnect()
    {
        $this->stomp = null;
    }

    public function isConnected()
    {
        return !is_null($this->stomp);
    }

    public function send($destination, StompMessage $message)
    {
        $this->assertIsConnected();

        $message = new \StompFrame(
            $message->getCommand(),
            $message->getHeaders(),
            $message->getBody()
        );

        try {
            if (!$this->stomp->send($destination, $message)) {
                $msg = 'Unable to send message';

                if ($this->stomp->error()) {
                    $msg .= '. ' . $this->stomp->error();
                }

                throw new MessagingException($msg);
            }
        } catch (\StompException $e) {
            throw new MessagingException('Unable to send message', 0, $e);
        }
    }

    public function subscribe($destination, array $headers = array())
    {
        $this->assertIsConnected();

        return $this->stomp->subscribe($destination, $headers);
    }

    public function unsubscribe($destination, array $headers = array())
    {
        $this->assertIsConnected();

        return $this->stomp->unsubscribe($destination, $headers);
    }

    public function hasMessage()
    {
        $this->assertIsConnected();

        return $this->stomp->hasFrame();
    }

    public function readMessage()
    {
        $this->assertIsConnected();

        try {
            $f = $this->stomp->readFrame();
        } catch (\StompException $e) {
            throw new MessagingException('Unable to read message', $this->getErrorCode($e), $e);
        }

        $result = null;

        if (!empty($f)) {
            $result = new StompMessage($f->body, $f->headers, $f->command);
        }

        return $result;
    }

    public function ack($messageId, array $headers = array())
    {
        $this->assertIsConnected();

        if (!$this->stomp->ack($messageId, $headers)) {
            $error = $this->stomp->error();
            $this->stomp->disconnect();
            throw new MessagingException('Unable to send ack. ' . $error);
        }
    }

    public function adaptMessage(Message $message, $command = 'SEND')
    {
        return new StompMessage($message->getBody(), array(), $command);
    }

    public function adaptSubscription(Subscription $subscription, $command = 'SUBSCRIBE')
    {
        $headers = array(
            'id' => $subscription->getId(),
            'ack' => self::$ackModes[$subscription->getAckMode()],
        );

        return new StompMessage(null, $headers, $command);
    }

    public function getReadTimeout()
    {
        return $this->readTimeout;
    }

    public function setReadTimeout($seconds)
    {
        $this->readTimeout = $seconds;

        if ($this->isConnected()) {
            $this->stomp->setReadTimeout($seconds);
        }
    }

    public function begin($transactionId, array $headers = array())
    {
        $this->assertIsConnected();

        return $this->stomp->begin($transactionId, $headers);
    }

    public function commit($transactionId, array $headers = array())
    {
        $this->assertIsConnected();

        return $this->stomp->commit($transactionId, $headers);
    }

    public function abort($transactionId, array $headers = array())
    {
        $this->assertIsConnected();

        return $this->stomp->abort($transactionId, $headers);
    }

    public function getSessionId()
    {
        $this->assertIsConnected();

        return $this->stomp->getSessionId();
    }

    public function getClientId()
    {
        return $this->clientId;
    }

    private function assertIsConnected()
    {
        if (!$this->isConnected()) {
            throw new MessagingException("Stomp client is not connected");
        }
    }

    private function getErrorCode(\StompException $ex)
    {
        return isset(self::$errorCodes[$ex->getMessage()]) ? self::$errorCodes[$ex->getMessage()] : 0;
    }
}
