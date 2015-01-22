<?php
namespace Vda\Messaging\Nop;

use \Vda\Messaging\IMessenger;
use \Vda\Messaging\IMessengerFactory;
use \Vda\Messaging\Message;
use \Vda\Messaging\Subscription;

/**
 * Simple no-operation messenger and its factory
 *
 * Use this messenger if you'd like to disable messaging functionality in code
 * depending on IMessenger and IMessengerFactory implementations.
 */
class NopMessengerFactory implements IMessengerFactory, IMessenger
{
    private static $instance;

    private function __construct()
    {
    }

    public static function getInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new self;
        }

        return self::$instance;
    }

    public function createMessenger($clientId)
    {
        return self::$instance;
    }

    public function send($destination, Message $message)
    {
    }

    public function subscribe(Subscription $subscription)
    {
    }

    public function unsubscribe(Subscription $subscription)
    {
    }

    public function receive($timeout = -1)
    {
        return null;
    }

    public function ack($messageId)
    {
    }
}
