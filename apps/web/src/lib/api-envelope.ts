import { randomUUID } from "crypto";
import { ZodError } from "zod";
import { NextResponse } from "next/server";

export interface ApiSuccess<T> {
  success: true;
  data: T;
  meta?: Record<string, unknown>;
}

export interface ApiFailure {
  success: false;
  error: {
    code: string;
    message: string;
    requestId: string;
    details?: unknown;
  };
}

export type ApiEnvelope<T> = ApiSuccess<T> | ApiFailure;

export function apiSuccess<T>(data: T, init?: { status?: number; meta?: Record<string, unknown> }) {
  return NextResponse.json<ApiSuccess<T>>(
    {
      success: true,
      data,
      ...(init?.meta ? { meta: init.meta } : {})
    },
    { status: init?.status ?? 200 }
  );
}

export function apiError(params: {
  status?: number;
  code: string;
  message: string;
  details?: unknown;
  requestId?: string;
}) {
  return NextResponse.json<ApiFailure>(
    {
      success: false,
      error: {
        code: params.code,
        message: params.message,
        requestId: params.requestId ?? randomUUID(),
        ...(params.details !== undefined ? { details: params.details } : {})
      }
    },
    { status: params.status ?? 500 }
  );
}

export function toApiErrorResponse(error: unknown) {
  if (error instanceof ZodError) {
    return apiError({
      status: 400,
      code: "invalid_request",
      message: "The request payload is invalid.",
      details: error.flatten()
    });
  }

  const maybeDbError = error as { code?: string; message?: string };
  if (maybeDbError.code === "P0001" && maybeDbError.message?.includes("Invalid review status transition")) {
    return apiError({
      status: 409,
      code: "invalid_review_transition",
      message: maybeDbError.message
    });
  }

  if (error instanceof Error && error.message.includes("Invalid review status transition")) {
    return apiError({
      status: 409,
      code: "invalid_review_transition",
      message: error.message
    });
  }

  if (error instanceof Error && error.message.includes("not found")) {
    return apiError({
      status: 404,
      code: "not_found",
      message: error.message
    });
  }

  return apiError({
    status: 500,
    code: "internal_error",
    message: "Unexpected server error.",
    details: error instanceof Error ? error.message : String(error)
  });
}
