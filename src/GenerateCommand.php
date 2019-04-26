<?php

namespace instantjay;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class GenerateCommand extends Command
{
    public function __construct()
    {
        parent::__construct('generate');
    }

    public function run(InputInterface $input, OutputInterface $output)
    {
        $generator = new Generator();
        $generator->execute(dirname(__DIR__));

        return 0;
    }
}