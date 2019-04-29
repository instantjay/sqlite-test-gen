<?php

namespace instantjay;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Query\QueryBuilder;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Stopwatch\Stopwatch;

class Generator
{
    protected $stopwatch;
    protected $logger;

    protected const NAMES = [
        'adam',
        'ben',
        'charlie',
        'dawson',
        'ernest',
        'ferdinand',
        'gunther',
        'harold',
        'ingram',
        'jack',
        'kevin',
        'lex',
        'martin',
        'nick',
        'olaf',
        'patric'
    ];

    protected const TAG_NAMES = [
        'smart',
        'beautiful',
        'slow',
        'smelly',
        'good-looking',
        'clever',
        'curious',
        'intelligent',
        'annoying',
        'self-centered'
    ];

    public function __construct()
    {
        //
        $this->stopwatch = new Stopwatch();
        $this->stopwatch->start('init');

        //
        $this->logger = new Logger('sqlite');
        $this->logger->pushHandler(
            new StreamHandler('php://stdout', Logger::DEBUG)
        );
    }

    /**
     * @param string $workingDirectoryPath
     * @return string
     * @throws \Doctrine\DBAL\DBALException
     */
    public function execute(string $workingDirectoryPath): string
    {
        //
        $filename = $this->generateFileName();
        $temporaryDatabaseFilePath = $workingDirectoryPath . '/tmp/' . $filename;

        $connection = $this->createConnection($temporaryDatabaseFilePath);

        //
        $this->createTables($connection);
        $this->insertFakeData($connection);

        //
        $fs = new Filesystem();
        $destinationDatabaseFilePath = $workingDirectoryPath . '/build/' . $filename;

        $this->logger->log(Logger::DEBUG, 'Attempting to move generated database file to ' . $destinationDatabaseFilePath);
        $fs->copy($temporaryDatabaseFilePath, $destinationDatabaseFilePath);

        $this->logger->log(Logger::DEBUG, 'Attempting to remove temp database file at ' . $temporaryDatabaseFilePath);
        $fs->remove($temporaryDatabaseFilePath);

        //
        $event = $this->stopwatch->stop('init');

        $memoryInMB = round($event->getMemory() / 1000000, 1);
        $this->logger->log(Logger::DEBUG, 'Execution took ' . $event->getDuration() . 'ms and consumed ' . $memoryInMB . 'MB');

        return $destinationDatabaseFilePath;
    }

    /**
     * @param string $temporaryDatabaseFilePath
     * @return \Doctrine\DBAL\Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    protected function createConnection(string $temporaryDatabaseFilePath): Connection
    {
        $this->logger->log(Logger::DEBUG, 'Attempting to create connection to ' . $temporaryDatabaseFilePath);

        $connectionConfiguration = new Configuration();
        $url = 'sqlite:///' . $temporaryDatabaseFilePath;
        $connectionParams = [
            'url' => $url
        ];

        return DriverManager::getConnection($connectionParams, $connectionConfiguration);
    }

    protected function createTables(Connection $connection): void
    {
        $connection->exec('
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(64) NOT NULL,
                CONSTRAINT unique_user_id UNIQUE (id)
            )
        ');

        $connection->exec('CREATE INDEX user_id_index ON users (id)');

        $connection->exec('
            CREATE TABLE tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title varchar(64) NOT NULL,
                CONSTRAINT unique_tag_id UNIQUE (id)
            )
        ');

        $connection->exec('CREATE INDEX tag_id_index ON tags (id)');

        $connection->exec('
            CREATE TABLE user_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id int(11) NOT NULL,
                tag_id int(11) NOT NULL,
                rank int(1) NOT NULL DEFAULT 0,
                CONSTRAINT unique_user_tag_id UNIQUE (id)
            )
        ');

        $connection->exec('CREATE INDEX user_tag_id_index ON user_tags (id)');
        $connection->exec('CREATE INDEX ut_uid_index ON user_tags (user_id)');
        $connection->exec('CREATE INDEX ut_tid_index ON user_tags (tag_id)');
        $connection->exec('CREATE INDEX ut_rank_index ON user_tags (rank)');
    }

    protected function insertFakeData(Connection $connection): void
    {
        try {
            $connection->beginTransaction();

            // Insert our known-length list of tags
            $tagCount = $this->insertTags($connection);
            $tagMaxId = $tagCount - 1;

            $desiredUserObjectCount = 100000;

            for ($i = 0; $i <= $desiredUserObjectCount; $i++) {
                $userId = $this->insertUser($connection);

                if (random_int(0, 1)) {
                    $tagId = random_int(0, $tagMaxId);
                    $rank = random_int(0, 9);

                    $this->associateUserWithTag($connection, $userId, $tagId, $rank);

                    //$this->logger->log(Logger::DEBUG, 'Associated User ' . $userId . ' with Tag ' . $tagId . ' (Rank ' . $rank . ')');
                }
            }

            $this->logger->log(Logger::DEBUG, 'Finished inserting ' . $desiredUserObjectCount . ' users.');

            $connection->commit();
        } catch (\Exception $e) {
            $connection->rollBack();

            $this->logger->log(Logger::DEBUG, 'Something went wrong and the queries were rolled back.', [
                'message' => $e->getMessage()
            ]);
        }
    }

    /**
     * @param Connection $connection
     * @return int the ID of the inserted user.
     */
    protected function insertUser(Connection $connection): int
    {
        $qb = new QueryBuilder($connection);

        $qb->insert('users');

        $randomNameId = array_rand(self::NAMES);
        $randomName = self::NAMES[$randomNameId];

        $values = [
            'name' => ':name',
        ];
        $qb->setParameter(':name', $randomName);

        $qb->values($values);
        $qb->execute();

        return $connection->lastInsertId();
    }

    /**
     * @param Connection $connection
     * @return int The amount of tags that we have available, which will be used to guess tag IDs to associate with our users.
     */
    protected function insertTags(Connection $connection): int
    {
        $qb = new QueryBuilder($connection);

        foreach (self::TAG_NAMES as $name) {
            $qb->insert('tags');

            $values = [
                'title' => ':title'
            ];
            $qb->setParameter(':title', $name);

            $qb->values($values);
            $qb->execute();
        }

        return count(self::TAG_NAMES);
    }

    protected function associateUserWithTag(Connection $connection, int $userId, int $tagId, int $rank = 0): void
    {
        $qb = new QueryBuilder($connection);
        $qb->insert('user_tags');

        $values = [
            'user_id' => $userId,
            'tag_id' => $tagId,
            'rank' => $rank
        ];

        $qb->values($values);
        $qb->execute();
    }

    /**
     * @return string
     * @throws \Exception
     */
    protected function generateFileName(): string
    {
        $currentDate = new \DateTime();

        return $currentDate->format('Ymd-His') . '-database.sqlite';
    }
}
