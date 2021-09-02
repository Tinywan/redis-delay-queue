<?php

namespace tauthz\tests;

use PHPUnit\Framework\TestCase;
use webman\permission\Permission;

class DatabaseAdapterTest extends TestCase
{
    public function testEnforce()
    {
        $this->assertTrue(Permission::enforce('alice', 'data1', 'read'));
    }
    
}
