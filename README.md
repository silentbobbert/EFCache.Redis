EFCache.Redis
=============

Extends EFCache by adding Redis support

I wanted to add L2 Cache to EF using Redis - there was nothing available at the time.

I found EFCache written by Pawel Kadluczka (moozzyk) over on CodePlex
http://efcache.codeplex.com/
http://www.nuget.org/packages/EntityFramework.Cache

Which gave me a very good starting point. I forked EFCache and asked Pawel to pull my changes, however for very good 
reasons, Pawel declined to do so. I added dependencies to the core EFCache project that Pawel didnt want to force
every consumer of EFCache to carry. He suggested I create my own NuGet Package, and use EFCache as a dependency
instead. So here it is, here is my code.