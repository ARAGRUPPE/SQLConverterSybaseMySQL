<?php


class ErrorHandler
{
    public static function handleError($errorSubject, $errorText)
    {
        echo $errorSubject . "<br>";
        echo $errorText;
    }
}