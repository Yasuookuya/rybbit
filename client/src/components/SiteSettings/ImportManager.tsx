"use client";

import { useMemo, useState, useRef, useEffect } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { DateTime } from "luxon";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Upload, FileText, AlertCircle, CheckCircle2, Clock, Loader2, Trash2, Info } from "lucide-react";
import { useGetSiteImports, useCreateSiteImport, useDeleteSiteImport } from "@/api/admin/import";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { IS_CLOUD } from "@/lib/const";
import { CSVWorkerManager } from "@/lib/import/csv-worker-manager";
import type { ImportProgress } from "@/lib/import/types";

interface ImportManagerProps {
  siteId: number;
  disabled: boolean;
}

const MAX_FILE_SIZE = IS_CLOUD ? 500 * 1024 * 1024 : 50 * 1024 * 1024 * 1024; // 500 MB for cloud, 50 GB for self-hosted
const CONFIRM_THRESHOLD = IS_CLOUD ? 100 * 1024 * 1024 : 1024 * 1024 * 1024; // Show confirmation for files > 100 MB (cloud) or > 1 GB (self-hosted)
const ALLOWED_FILE_TYPES = ["text/csv"];
const ALLOWED_EXTENSIONS = [".csv"];

const importFormSchema = z.object({
  file: z
    .custom<FileList>()
    .refine(files => files.length === 1, "Please select a file")
    .refine(
      files => {
        const file = files[0];
        return file && file.size <= MAX_FILE_SIZE;
      },
      `File size must be less than ${MAX_FILE_SIZE / 1024 / 1024} MB`
    )
    .refine(files => {
      const file = files[0];
      if (!file) return false;
      const extension = "." + file.name.split(".").pop()?.toLowerCase();
      return ALLOWED_EXTENSIONS.includes(extension) || ALLOWED_FILE_TYPES.includes(file.type);
    }, "Only CSV files are accepted")
    .refine(files => {
      const file = files[0];
      return file && file.name.length <= 255;
    }, "Filename is too long"),
});

type ImportFormData = z.infer<typeof importFormSchema>;

function formatFileSize(bytes: number): string {
  const sizeInMB = bytes / 1024 / 1024;
  const sizeInGB = bytes / 1024 / 1024 / 1024;

  if (sizeInGB < 1) {
    return `${sizeInMB.toFixed(2)} MB`;
  } else {
    return `${sizeInGB.toFixed(2)} GB`;
  }
}

export function ImportManager({ siteId, disabled }: ImportManagerProps) {
  const [showConfirmDialog, setShowConfirmDialog] = useState(false);
  const [pendingImportData, setPendingImportData] = useState<ImportFormData | null>(null);
  const [deleteImportId, setDeleteImportId] = useState<string | null>(null);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [importProgress, setImportProgress] = useState<ImportProgress | null>(null);
  const [showProgressDialog, setShowProgressDialog] = useState(false);
  const workerManagerRef = useRef<CSVWorkerManager | null>(null);

  const { data, isLoading, error } = useGetSiteImports(siteId);
  const createImportMutation = useCreateSiteImport(siteId);
  const deleteMutation = useDeleteSiteImport(siteId);

  const {
    register,
    handleSubmit,
    watch,
    reset,
    formState: { errors, isValid },
  } = useForm<ImportFormData>({
    resolver: zodResolver(importFormSchema),
    mode: "onChange",
    defaultValues: {
      file: Object.assign(new DataTransfer().files, {}),
    },
  });

  const fileList = watch("file");
  const selectedFile = fileList?.[0];

  // Cleanup worker on component unmount
  useEffect(() => {
    return () => {
      workerManagerRef.current?.terminate();
    };
  }, []);

  const onSubmit = (data: ImportFormData) => {
    const file = data.file[0];
    if (!file) return;

    if (file.size > CONFIRM_THRESHOLD) {
      setPendingImportData(data);
      setShowConfirmDialog(true);
    } else {
      executeImport(data);
    }
  };

  const executeImport = (data: ImportFormData) => {
    const file = data.file[0];
    if (!file) return;

    // Step 1: Create import record and get allowed date range
    createImportMutation.mutate(undefined, {
      onSuccess: response => {
        const { importId, allowedDateRange } = response.data;

        // Step 2: Initialize worker manager
        workerManagerRef.current = new CSVWorkerManager(
          progress => {
            setImportProgress(progress);
          },
          (success, message) => {
            console.log(success ? "✅" : "❌", message);
            // Import list will auto-refresh due to polling in useGetSiteImports
          }
        );

        // Step 3: Show progress dialog
        setShowProgressDialog(true);

        // Step 4: Start CSV parsing and upload with allowed date range
        workerManagerRef.current.startImport(
          file,
          siteId,
          importId,
          allowedDateRange.earliestAllowedDate,
          allowedDateRange.latestAllowedDate
        );

        // Reset form
        reset();
      },
      onError: error => {
        console.error("Failed to create import:", error);
      },
    });

    setShowConfirmDialog(false);
  };

  const handleDeleteClick = (importId: string) => {
    setDeleteImportId(importId);
    setShowDeleteDialog(true);
  };

  const handleDeleteConfirm = () => {
    if (deleteImportId) {
      deleteMutation.mutate(deleteImportId, {
        onSuccess: () => {
          setDeleteImportId(null);
          setShowDeleteDialog(false);
        },
        onError: () => {
          setDeleteImportId(null);
          setShowDeleteDialog(false);
        },
      });
    }
  };

  const getStatusInfo = (status: string) => {
    switch (status.toLowerCase()) {
      case "completed":
        return {
          color: "bg-green-100 text-green-800 border-green-200",
          icon: CheckCircle2,
          label: "Completed",
        };
      case "failed":
        return {
          color: "bg-red-100 text-red-800 border-red-200",
          icon: AlertCircle,
          label: "Failed",
        };
      case "processing":
        return {
          color: "bg-blue-100 text-blue-800 border-blue-200",
          icon: Loader2,
          label: "Processing",
        };
      case "pending":
        return {
          color: "bg-yellow-100 text-yellow-800 border-yellow-200",
          icon: Clock,
          label: "Pending",
        };
      default:
        return {
          color: "bg-gray-100 text-gray-800 border-gray-200",
          icon: Clock,
          label: status,
        };
    }
  };

  const sortedImports = useMemo(() => {
    if (!data?.data) {
      return [];
    }

    return [...data.data].sort((a, b) => {
      const aTime = new Date(a.startedAt).getTime();
      const bTime = new Date(b.startedAt).getTime();
      return bTime - aTime;
    });
  }, [data?.data]);

  // Check if there's an active import (cloud only)
  const hasActiveImport =
    IS_CLOUD && sortedImports.some(imp => imp.status === "pending" || imp.status === "processing");

  const isImportDisabled = !isValid || createImportMutation.isPending || disabled || hasActiveImport;

  return (
    <div className="space-y-6">
      {/* Import Section */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Upload className="h-5 w-5" />
            Import Data
          </CardTitle>
          <CardDescription>
            Import data from other analytics platforms. Supports CSV files up to {formatFileSize(MAX_FILE_SIZE)}.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Active Import Warning */}
          {hasActiveImport && (
            <Alert>
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>
                You have an active import in progress. Please wait for it to complete before starting a new import.
              </AlertDescription>
            </Alert>
          )}

          <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
            {/* File Upload */}
            <div className="space-y-2">
              <Label htmlFor="file" className="flex items-center gap-2">
                <FileText className="h-4 w-4" />
                CSV File
              </Label>
              <Input
                type="file"
                accept=".csv"
                multiple={false}
                {...register("file")}
                disabled={disabled || createImportMutation.isPending || hasActiveImport}
              />
              {selectedFile && (
                <p className="text-sm text-muted-foreground">
                  Selected: {selectedFile.name} ({formatFileSize(selectedFile.size)})
                </p>
              )}
              {errors.file && <p className="text-sm text-red-600">{errors.file.message as string}</p>}
            </div>

            {/* Import Button */}
            <Button type="submit" disabled={isImportDisabled} className="w-full sm:w-auto">
              {createImportMutation.isPending ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Importing...
                </>
              ) : (
                <>
                  <Upload className="mr-2 h-4 w-4" />
                  Import
                </>
              )}
            </Button>
          </form>

          {/* Import Error */}
          {createImportMutation.isError && (
            <Alert variant="destructive">
              <div className="flex items-center gap-2">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>
                  {createImportMutation.error.message || "Failed to import file. Please try again."}
                </AlertDescription>
              </div>
            </Alert>
          )}

          {/* Success Message */}
          {createImportMutation.isSuccess && (
            <Alert className="border-green-200 bg-green-50">
              <div className="flex items-center gap-2">
                <CheckCircle2 className="h-4 w-4 text-green-600" />
                <AlertDescription>File uploaded successfully and is being processed.</AlertDescription>
              </div>
            </Alert>
          )}

          {/* Delete Success Message */}
          {deleteMutation.isSuccess && (
            <Alert className="border-green-200 bg-green-50">
              <div className="flex items-center gap-2">
                <CheckCircle2 className="h-4 w-4 text-green-600" />
                <AlertDescription>Import deleted successfully.</AlertDescription>
              </div>
            </Alert>
          )}

          {/* Delete Error Message */}
          {deleteMutation.isError && (
            <Alert variant="destructive">
              <div className="flex items-center gap-2">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>
                  {deleteMutation.error.message || "Failed to delete import. Please try again."}
                </AlertDescription>
              </div>
            </Alert>
          )}
        </CardContent>
      </Card>

      {/* Import History */}
      <Card>
        <CardHeader>
          <CardTitle>Import History</CardTitle>
          <CardDescription>Track the status of your data imports</CardDescription>
        </CardHeader>
        <CardContent>
          {isLoading && !data ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-6 w-6 animate-spin mr-2" />
              <span>Loading import history...</span>
            </div>
          ) : error ? (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>Failed to load import history. Please try refreshing the page.</AlertDescription>
            </Alert>
          ) : !data?.data?.length ? (
            <div className="text-center py-8 text-muted-foreground">
              <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
              <p>No imports yet</p>
              <p className="text-sm">Upload a CSV file to get started</p>
            </div>
          ) : (
            <div className="rounded-md">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Started At</TableHead>
                    <TableHead>Platform</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead className="text-right">Events</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sortedImports.map(imp => {
                    const statusInfo = getStatusInfo(imp.status);
                    const StatusIcon = statusInfo.icon;
                    const startedAt = DateTime.fromISO(imp.startedAt).toFormat("MMM dd, yyyy HH:mm");

                    return (
                      <TableRow key={imp.importId}>
                        <TableCell className="font-medium">{startedAt}</TableCell>
                        <TableCell>
                          <Badge variant="outline" className="capitalize">
                            {imp.platform || "N/A"}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <Badge className={`${statusInfo.color} flex items-center gap-1`}>
                              <StatusIcon className={`h-3 w-3 ${imp.status === "processing" ? "animate-spin" : ""}`} />
                              {statusInfo.label}
                            </Badge>
                            {imp.errorMessage && (
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <button type="button" className="text-red-600 hover:text-red-700">
                                      <Info className="h-4 w-4" />
                                    </button>
                                  </TooltipTrigger>
                                  <TooltipContent side="right" className="max-w-sm">
                                    <p className="text-sm">{imp.errorMessage}</p>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            )}
                          </div>
                        </TableCell>
                        <TableCell className="text-right">{imp.importedEvents.toLocaleString()}</TableCell>
                        <TableCell className="text-right">
                          {(imp.status === "completed" || imp.status === "failed") && (
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => handleDeleteClick(imp.importId)}
                              disabled={disabled || deleteMutation.isPending}
                              className="h-8 w-8 p-0"
                            >
                              {deleteMutation.isPending && deleteMutation.variables === imp.importId ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                              ) : (
                                <Trash2 className="h-4 w-4" />
                              )}
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Confirmation Dialog */}
      <AlertDialog open={showConfirmDialog} onOpenChange={setShowConfirmDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Confirm Large File Import</AlertDialogTitle>
            <AlertDialogDescription>
              You're about to import a large file ({selectedFile ? formatFileSize(selectedFile.size) : "?"}). This may
              take several minutes to process.
              {!IS_CLOUD && selectedFile && (
                <>
                  {" "}
                  Ensure your server has at least {Math.ceil((selectedFile.size / 1024 / 1024 / 1024) * 2)} GB of free
                  disk space.
                </>
              )}{" "}
              Are you sure you want to continue?
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={() => pendingImportData && executeImport(pendingImportData)}>
              Yes, Import File
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Import</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete this import? This action cannot be undone. The import data and associated
              files will be permanently removed.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleDeleteConfirm} className="bg-red-600 hover:bg-red-700">
              Delete Import
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Import Progress Dialog */}
      <AlertDialog open={showProgressDialog} onOpenChange={setShowProgressDialog}>
        <AlertDialogContent className="max-w-2xl">
          <AlertDialogHeader>
            <AlertDialogTitle>Import Progress</AlertDialogTitle>
            <AlertDialogDescription>
              {importProgress?.status === "parsing" && "Parsing CSV file..."}
              {importProgress?.status === "uploading" && "Uploading batches to server..."}
              {importProgress?.status === "completed" && "Import completed!"}
              {importProgress?.status === "failed" && "Import failed"}
            </AlertDialogDescription>
          </AlertDialogHeader>

          <div className="space-y-4">
            {/* Progress Stats */}
            <div className="grid grid-cols-3 gap-4">
              <div>
                <p className="text-sm text-muted-foreground">Parsed Rows</p>
                <p className="text-2xl font-bold">{importProgress?.parsedRows.toLocaleString()}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Skipped Rows</p>
                <p className="text-2xl font-bold">{importProgress?.skippedRows.toLocaleString()}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Imported Events</p>
                <p className="text-2xl font-bold">{importProgress?.importedEvents.toLocaleString()}</p>
              </div>
            </div>

            {/* Errors */}
            {importProgress && importProgress.errors.length > 0 && (
              <div className="max-h-40 overflow-y-auto">
                <p className="text-sm font-medium text-red-600 mb-2">Errors ({importProgress.errors.length}):</p>
                {importProgress.errors.slice(0, 10).map((error, idx) => (
                  <p key={idx} className="text-xs text-red-600">
                    {error.message}
                  </p>
                ))}
                {importProgress.errors.length > 10 && (
                  <p className="text-xs text-muted-foreground">...and {importProgress.errors.length - 10} more</p>
                )}
              </div>
            )}
          </div>

          <AlertDialogFooter>
            {importProgress?.status === "completed" || importProgress?.status === "failed" ? (
              <AlertDialogAction onClick={() => setShowProgressDialog(false)}>Close</AlertDialogAction>
            ) : (
              <AlertDialogCancel
                onClick={() => {
                  workerManagerRef.current?.terminate();
                  setShowProgressDialog(false);
                }}
              >
                Cancel Import
              </AlertDialogCancel>
            )}
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
