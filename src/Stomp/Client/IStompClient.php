<?php
namespace Vda\Messaging\Stomp\Client;

use \Vda\Messaging\Message;
use \Vda\Messaging\Stomp\StompMessage;
use \Vda\Messaging\Subscription;

interface IStompClient
{
    public function connect(array $headers = array());
    public function disconnect();
    public function isConnected();

    /**
     * Sends $message to $destination
     *
     * @param string $destination
     * @param StompMessage $message
     */
    public function send($destination, StompMessage $message);

    /**
     * Subscribe to $destination queue or topic
     *
     * @param String $destination
     * @param array $headers
     */
    public function subscribe($destination, array $headers = array());

    /**
     * Unsubscribe from $destination
     *
     * @param string $destination
     * @param array $headers
     */
    public function unsubscribe($destination, array $headers = array());

    /**
     * Indicates whether or not there is a frame ready to read
     *
     * @return boolean
     */
    public function hasMessage();

    /**
     * Read next frame. Will return null if nothing was read during connection timeout
     *
     * @return \Vda\Messaging\Stomp\StompMessage
     */
    public function readMessage();

    /**
     * Acknowleges message consumption
     *
     * @param string $messageId
     * @param array $headers
     */
    public function ack($messageId, array $headers = array());

    /**
     * Convert Message to StompMessage
     *
     * @param Message $message
     * @param string $command Stomp command to set on frame
     * @return StompMessage
     */
    public function adaptMessage(Message $message, $command = 'SEND');

    /**
     * Convert Subscription to StompMessage
     *
     * @param Subscription $subscription
     * @param string $command Stomp command to set on frame
     * @return StompMessage
     */
    public function adaptSubscription(Subscription $subscription, $command = 'SUBSCRIBE');

    public function getClientId();
    public function getReadTimeout();
    public function setReadTimeout($seconds);
    public function begin($transactionId, array $headers = array());
    public function commit($transactionId, array $headers = array());
    public function abort($transactionId, array $headers = array());
    public function getSessionId();
}
