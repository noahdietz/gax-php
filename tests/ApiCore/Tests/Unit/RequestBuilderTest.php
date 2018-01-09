<?php
/**
 * Copyright 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Google\ApiCore\Tests\Unit;

use Google\ApiCore\RequestBuilder;
use Google\ApiCore\Tests\Mocks\MockRequestBody;
use PHPUnit\Framework\TestCase;

/**
 * @group core
 */
class RequestBuilderTest extends TestCase
{
    const SERVICE_NAME = 'test.interface.v1.api';

    public function setUp()
    {
        $this->builder = new RequestBuilder(
            'www.example.com',
            __DIR__ . '/testdata/test_service_rest_client_config.php'
        );
    }

    public function testMethodWithUrlPlaceholder()
    {
        $message = new MockRequestBody();
        $message->setName('message/foo');

        $request = $this->builder->build(self::SERVICE_NAME . '/MethodWithUrlPlaceholder', $message);
        $uri = $request->getUri();

        $this->assertEquals('/v1/message/foo', $uri->getPath());
        $this->assertEquals('number=0', $uri->getQuery());
        $this->assertEquals('', (string) $request->getBody());
    }

    public function testMethodWithBody()
    {
        $message = new MockRequestBody();
        $message->setName('message/foo');
        $nestedMessage = new MockRequestBody();
        $nestedMessage->setName('nested/foo');
        $message->setNestedMessage($nestedMessage);

        $request = $this->builder->build(self::SERVICE_NAME . '/MethodWithBody', $message);
        $uri = $request->getUri();

        $this->assertEquals('/v1/message/foo', $uri->getPath());
        $this->assertEquals('', $uri->getQuery());
        $this->assertEquals(
            ['name' => 'message/foo', 'nestedMessage' => ['name' => 'nested/foo']],
            json_decode($request->getBody(), true)
        );
    }

    public function testMethodWithNestedMessageAsBody()
    {
        $message = new MockRequestBody();
        $message->setName('message/foo');
        $nestedMessage = new MockRequestBody();
        $nestedMessage->setName('nested/foo');
        $message->setNestedMessage($nestedMessage);

        $request = $this->builder->build(self::SERVICE_NAME . '/MethodWithNestedMessageAsBody', $message);
        $uri = $request->getUri();

        $this->assertEquals('/v1/message/foo', $uri->getPath());
        $this->assertEquals('number=0', $uri->getQuery());
        $this->assertEquals(
            ['name' => 'nested/foo'],
            json_decode($request->getBody(), true)
        );
    }

    public function testMethodWithNestedUrlPlaceholder()
    {
        $message = new MockRequestBody();
        $message->setName('message/foo');
        $nestedMessage = new MockRequestBody();
        $nestedMessage->setName('nested/foo');
        $message->setNestedMessage($nestedMessage);

        $request = $this->builder->build(self::SERVICE_NAME . '/MethodWithNestedUrlPlaceholder', $message);
        $uri = $request->getUri();

        $this->assertEquals('/v1/nested/foo', $uri->getPath());
        $this->assertEquals('', $uri->getQuery());
        $this->assertEquals(
            ['name' => 'message/foo', 'nestedMessage' => ['name' => 'nested/foo']],
            json_decode($request->getBody(), true)
        );
    }

    public function testMethodWithUrlRepeatedField()
    {
        $message = new MockRequestBody();
        $message->setName('message/foo');
        $message->setRepeatedField(['bar1', 'bar2']);

        $request = $this->builder->build(self::SERVICE_NAME . '/MethodWithUrlPlaceholder', $message);
        $uri = $request->getUri();

        $this->assertEquals('/v1/message/foo', $uri->getPath());
        $this->assertEquals('number=0&repeated_field=bar1&repeated_field=bar2', $uri->getQuery());
        $this->assertEquals('', (string) $request->getBody());
    }

    public function testMethodWithHeaders()
    {
        $message = new MockRequestBody();
        $message->setName('message/foo');

        $request = $this->builder->build(self::SERVICE_NAME . '/MethodWithUrlPlaceholder', $message, [
            'header1' => 'value1',
            'header2' => 'value2']
        );

        $this->assertEquals('value1', $request->getHeaderLine('header1'));
        $this->assertEquals('value2', $request->getHeaderLine('header2'));
        $this->assertEquals('application/json', $request->getHeaderLine('Content-Type'));
    }

    /**
     * @expectedException RuntimeException
     * @expectedExceptionMessage Failed to build request, as the provided path (myResource/doesntExist) was not found in the configuration.
     */
    public function testThrowsExceptionWithNonExistantMethod()
    {
        $message = new MockRequestBody();
        $this->builder->build('myResource/doesntExist', $message);
    }
}
