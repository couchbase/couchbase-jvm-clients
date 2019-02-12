## Tracking RFC on these areas and may need to make changes
- Decision may be made to defer errors until the operation level, e.g. opening resources would always succeed
- Projections IMO need some design iteration before implementing.
- Query also IMO needs some love.  Error handling may change, at least.  Plus unclear if async/blocking needs to stream rows.

## Material to recycle for 'choosing a json lib' docs later

  /**
    * Scala already has a number of great, popular JSON libraries.  Instead of re-inventing the wheel, the philosophy
    * is to provide direct support for a handful of these, and extend that support based on what our customers request.
    */

  /**
    * upickle is a lightweight fast serialization library that includes a good JSON lib called usjon.
    *
    * upickle includes a JSON AST which is very friendly to use.  Unusually for Scala libs, it's mutable - the author
    * argues that JSON tends to be very short-lived and used in just one scope, so it doesn't gain much from
    * immutability - while mutability makes it much easier to modify JSON.
    *
    * It's also highly performant and has a perhaps unique property of being able to encode/decode directly into Array[Byte],
    * without needed to go through JSON AST first.
    *
    * I really like this lib and may make this the default for all examples.
    */

  /**
    * Like most Scala JSON libs, upickle can convert Scala case classes directly to/from JSON.
    *
    * But like almost all JSON libs I've found (except circe, covered below), it requires the app to write a small
    * amount of boilerplate to support it.
    */
  /**
    * As a fallback, support Array[Byte] directly
    * Here we'll use uJson to encode and decode Array[Byte], but any JSON lib that can support that could be used
    * When given Array[Byte], the lib will assume it's JSON and send the corresponding flag
    */


